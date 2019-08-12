# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------
import re
import itertools
import getpass
from .kql_proxy import KqlResponse
import functools
from .constants import ConnStrKeys
from .my_utils import get_valid_filename, adjust_path
from .parser import Parser
from .log import logger



_FQN_KUSTO_CLUSTER_PATTERN = re.compile(r"(http(s?)\:\/\/)?(?P<cname>.*)\.kusto\.(windows\.net|chinacloudapi.cn|cloudapi.de|usgovcloudapi.net)$")
_FQN_DRAFT_PROXY_CLUSTER_PATTERN = re.compile(r"http(s?)\:\/\/ade\.(int\.)?(?P<io>(applicationinsights|loganalytics))\.io\/subscriptions\/(?P<subscription>.*)$")
_FQN_ARIA_KUSTO_CLUSTER_PATTERN = re.compile(r"http(s?)\:\/\/kusto\.aria\.microsoft\.com$")

class KqlEngine(object):

    # Object constructor
    def __init__(self):
        self.bind_url = None
        self._parsed_conn = {}
        self.database_name = None
        self.database_friendly_name = None
        self.cluster_name = None
        self.cluster_friendly_name = None
        self.alias = None
        self.client = None
        self.options = {}

        self.validated = None
        self.conn_name = None

    def __eq__(self, other):
        return self.bind_url and self.bind_url == other.bind_url

    def is_validated(self):
        return self.validated == True

    def set_validation_result(self, result):
        self.validated = result == True

    def get_alias(self):
        return self.alias

    def get_database(self):
        if not self.database_name:
            raise KqlEngineError("Database is not defined.")
        return self.database_name

    def get_cluster(self):
        if not self.cluster_name:
            raise KqlEngineError("Cluster is not defined.")
        return self.cluster_name

    def get_cluster_friendly_name(self):
        if not self.cluster_friendly_name:
            raise KqlEngineError("Cluster friendly name is not defined.")
        return self.cluster_friendly_name

    def get_database_friendly_name(self):
        if not self.database_friendly_name:
            raise KqlEngineError("Database friendly name is not defined.")
        return self.database_friendly_name


    def get_conn_name(self):
        if self.conn_name:
            return self.conn_name
        # print('=',self.alias,'=', self.cluster_friendly_name, '=', self.database_friendly_name)
        if self.alias and self.cluster_friendly_name and self.database_friendly_name:
            self.conn_name = "{0}@{1}".format(self.alias, self.cluster_friendly_name)
            return self.conn_name
        else:
            raise KqlEngineError("Database and/or cluster is not defined.")


    def createDatabaseFriendlyName(self, dname):
        return get_valid_filename(dname)

    def createClusterFriendlyName(self, cname):
        match = _FQN_KUSTO_CLUSTER_PATTERN.match(self.cluster_name)
        if match:
            return match.group("cname")

        name = cname[:-1] if cname[-1] == "/" else cname

        match = _FQN_DRAFT_PROXY_CLUSTER_PATTERN.match(name)
        if match:
            components = match.group("subscription").split("/")
            resource_name = "app" if match.group("io") == "applicationinsights" else "workspace"
            name = "{0}s_in_subscription_{1}".format(resource_name, components[0])

            if len(components) >= 3:
                key = components[1].lower
                if key == "resourcegroups":
                    name = "{0}_resourcegroup_{1}".format(name, components[2])
                    if len(components) >= 5:
                        name = "{0}_{1}_{2}".format(name, resource_name, components[-1])
                else:
                    name = "{0}_{1}_{2}".format(name, resource_name, components[-1])
        elif _FQN_ARIA_KUSTO_CLUSTER_PATTERN.match(name):
            name = "adx_aria"
        else:
            if name.startswith("https://"):
                name = name[8:]
            elif name.startswith("http://"):
                name = name[7:]
            name = name.replace("/", "_").replace(".", "_").replace(":", "_").replace("-", "_")
        return name


    def get_client(self):
        return self.client

    def client_execute(self, query, user_namespace=None, **options):
        if query.strip():
            client = self.get_client()
            if not client:
                raise KqlEngineError("Client is not defined.")
            return client.execute(self.get_database(), query, accept_partial_results=False, **options)

    def execute(self, query, user_namespace=None, **options):
        if query.strip():
            response = self.client_execute(query, user_namespace, **options)
            # print(response.json_response)
            return KqlResponse(response, **options)

    def validate(self, **options):
        client = self.get_client()
        if not client:
            raise KqlEngineError("Client is not defined.")
        query = "range c from 1 to 10 step 1 | count"
        response = client.execute(self.get_database(), query, accept_partial_results=False, **options)
        # print(response.json_response)
        table = KqlResponse(response, **options).tables[0]
        if table.rowcount() != 1 or table.colcount() != 1 or [r for r in table.fetchall()][0][0] != 10:
            raise KqlEngineError("Client failed to validate connection.")

    _CREDENTIAL_KEYS = {
        ConnStrKeys.TENANT,
        ConnStrKeys.AAD_URL,
        ConnStrKeys.USERNAME,
        ConnStrKeys.CLIENTID,
        ConnStrKeys.CERTIFICATE,
        ConnStrKeys.CLIENTSECRET,
        ConnStrKeys.APPKEY,
        ConnStrKeys.PASSWORD,
        ConnStrKeys.CERTIFICATE_THUMBPRINT,
    }
    _SECRET_KEYS = {ConnStrKeys.CLIENTSECRET, ConnStrKeys.APPKEY, ConnStrKeys.PASSWORD, ConnStrKeys.CERTIFICATE_THUMBPRINT}
    _NOT_INHERITABLE_KEYS = {ConnStrKeys.APPKEY, ConnStrKeys.ALIAS}
    _OPTIONAL_KEYS = {ConnStrKeys.TENANT,ConnStrKeys.AAD_URL, ConnStrKeys.DATA_SOURCE_URL, ConnStrKeys.ALIAS, ConnStrKeys.CLIENTID}
    _INHERITABLE_KEYS = {ConnStrKeys.CLUSTER, ConnStrKeys.TENANT,ConnStrKeys.AAD_URL, ConnStrKeys.DATA_SOURCE_URL}
    _EXCLUDE_FROM_URL_KEYS = {ConnStrKeys.DATABASE, ConnStrKeys.ALIAS}
    _SHOULD_BE_NULL_KEYS = {ConnStrKeys.CODE, ConnStrKeys.ANONYMOUS}

    def _parse_common_connection_str(
        self, conn_str: str, current, uri_schema_name, mandatory_key: str, valid_keys_combinations: list, user_ns: dict
    ):

        logger().debug("kql_engine.py -_parse_common_connection_str - params:  conn_str: {0}; current: {1}, uri_schema_name: {2};mandatory_key: {3}, valid_keys_combinations: {4}, user_ns: {5}".format(conn_str, current, uri_schema_name, mandatory_key, valid_keys_combinations, user_ns))

        error_msg = "invalid connection string: {0}".format(conn_str)
        rest = conn_str[conn_str.find("://")+3:].strip()

        # get key/values in connection string
        parsed_conn_kv = Parser.parse_and_get_kv_string(rest, user_ns)
        logger().debug("kql_engine.py -_parse_common_connection_str - parsed_conn_kv:   {0}   return of Parser.parse_and_get_kv_string".format(parsed_conn_kv))
        # In case certificate_pem_file was specified instead of certificate.
        pem_file_name = parsed_conn_kv.get(ConnStrKeys.CERTIFICATE_PEM_FILE)
        if pem_file_name is not None:
            pem_file_name = adjust_path(pem_file_name)
            with open(pem_file_name, "r") as pem_file:
                parsed_conn_kv[ConnStrKeys.CERTIFICATE] = pem_file.read()
                del parsed_conn_kv[ConnStrKeys.CERTIFICATE_PEM_FILE]

        matched_keys_set = set(parsed_conn_kv.keys())
        logger().debug("kql_engine.py -_parse_common_connection_str - matched_keys_set:  {0}".format(matched_keys_set))


        # check for unknown keys
        all_keys = set(itertools.chain(*valid_keys_combinations))
        unknonw_keys_set = matched_keys_set.difference(all_keys)
        if len(unknonw_keys_set) > 0:
            raise ValueError("{0}, detected unknown keys: {1}.".format(error_msg, unknonw_keys_set))

        # check that mandatory key in matched set
        if mandatory_key not in matched_keys_set:
            logger().debug("kql_engine.py -_parse_common_connection_str - the following mandatory_key is missing:  {0}".format(mandatory_key))
            raise KqlEngineError("{0}, mandatory key {1} is missing.".format(error_msg, mandatory_key))

        # find a valid combination for the set
        valid_combinations = [c for c in valid_keys_combinations if matched_keys_set.issubset(c)]
        logger().debug("kql_engine.py -_parse_common_connection_str - found these valid_combinations :  {0}".format(valid_combinations))


        # in case of ambiguity, assume it is based on current connection, resolve by copying missing values from current
        if len(valid_combinations) > 1:
            if current is not None:
                for k, v in current._parsed_conn.items():
                    if k not in matched_keys_set and k not in self._NOT_INHERITABLE_KEYS:
                        parsed_conn_kv[k] = v
                        matched_keys_set.add(k)
                for k in self._CREDENTIAL_KEYS.intersection(matched_keys_set):
                    if parsed_conn_kv[k] != current._parsed_conn.get(k):
                        raise KqlEngineError("{0}, missing keys.".format(error_msg))
        valid_combinations = [c for c in valid_combinations if matched_keys_set.issubset(c)]

        logger().debug("kql_engine.py -_parse_common_connection_str - inherited from current and now valid_combinations is :  {0}".format(valid_combinations))


        # only one combination can be accepted
        if len(valid_combinations) == 0:
            logger().debug("kql_engine.py -_parse_common_connection_str - valid_combinations is empty :  {0}".format(valid_combinations))
            raise KqlEngineError("{0}, not a valid keys set, missing keys.".format(error_msg))

        conn_keys_list = None
        # if still too many choose the shortest
        if len(valid_combinations) > 1:
            conn_keys_list =  min(valid_combinations, key=len)
        else:
            conn_keys_list = valid_combinations[0]
        logger().debug("kql_engine.py -_parse_common_connection_str - chose conn_keys_list - shortest out of valid_combinations :  {0}".format(conn_keys_list))

        if conn_keys_list is None:
            raise KqlEngineError("{0}, not a valid keys set, missing keys.".format(error_msg))

        conn_keys_set = set(conn_keys_list)

        # in case inheritable fields are missing inherit from current if exist
        inherit_keys_set = self._INHERITABLE_KEYS.intersection(conn_keys_set).difference(matched_keys_set)
        if len(inherit_keys_set) > 1:
            if current is not None:
                for k in inherit_keys_set:
                    v = current._parsed_conn.get(k)
                    if v is not None:
                        parsed_conn_kv[k] = v
                        matched_keys_set.add(k)
        logger().debug("kql_engine.py -_parse_common_connection_str - add inherited to conn_keys_set and now inherit_keys_set :  {0}".format(inherit_keys_set))

        # make sure that all required keys are in set
        secret_key_set = self._SECRET_KEYS.intersection(conn_keys_set)
        missing_set = conn_keys_set.difference(matched_keys_set).difference(secret_key_set).difference(self._OPTIONAL_KEYS)
        if len(missing_set) > 0:
            logger().debug("kql_engine.py -_parse_common_connection_str - the following required key is missing :  {0}".format(missing_set))

            raise KqlEngineError("{0}, missing {1}.".format(error_msg, missing_set))
        # special case although tenant in _OPTIONAL_KEYS
        if parsed_conn_kv.get(ConnStrKeys.TENANT) is None and ConnStrKeys.CLIENTID in conn_keys_set:
            logger().debug("kql_engine.py -_parse_common_connection_str - special case- if clientId exists but tenant doesnt")

            raise KqlEngineError("{0}, missing tenant key/value.".format(error_msg))

        # make sure that all required keys are with proper value
        for key in matched_keys_set:  # .difference(secret_key_set).difference(self._SHOULD_BE_NULL_KEYS):
            if key in self._SHOULD_BE_NULL_KEYS:
                if parsed_conn_kv[key] != "":
                    raise KqlEngineError("{0}, key {1} must be empty.".format(error_msg, key))
            elif key not in self._SECRET_KEYS:
                if parsed_conn_kv[key] == "<{0}>".format(key) or parsed_conn_kv[key] =="":
                    raise KqlEngineError("{0}, key {1} cannot be empty or set to <{2}>.".format(error_msg, key, key))
        logger().debug("kql_engine.py -_parse_common_connection_str - made sure that all required keys are with proper value :  {0}".format(matched_keys_set))

        # set attributes
        self.cluster_name = parsed_conn_kv.get(ConnStrKeys.CLUSTER) or uri_schema_name
        if self.cluster_name is not None:
            if len(self.cluster_name) < 1:
                raise KqlEngineError("{0}, key {1} cannot be empty.".format(error_msg, ConnStrKeys.CLUSTER))
            self.cluster_friendly_name = self.createClusterFriendlyName(self.cluster_name)
        logger().debug("kql_engine.py -_parse_common_connection_str - setting attributes- cluster_name :  {0}".format(self.cluster_name))
        logger().debug("kql_engine.py -_parse_common_connection_str - setting attributes- cluster_friendly_name :  {0}".format(self.cluster_friendly_name))

        self.database_name = parsed_conn_kv.get(mandatory_key)
        if self.database_name is not None:
            if len(self.database_name) < 1:
                raise KqlEngineError("{0}, key {1} cannot be empty <{2}>.".format(error_msg, mandatory_key, self.database_name))
            self.database_friendly_name = self.createDatabaseFriendlyName(self.database_name)
        logger().debug("kql_engine.py -_parse_common_connection_str - setting attributes- database_name :  {0}".format(self.database_name))
        logger().debug("kql_engine.py -_parse_common_connection_str - setting attributes- database_friendly_name :  {0}".format(self.database_friendly_name))

        self.alias = parsed_conn_kv.get(ConnStrKeys.ALIAS) or self.database_friendly_name
        if self.alias is not None:
            if len(self.alias) < 1 or self.alias != get_valid_filename(self.alias):
                raise KqlEngineError("{0}, key {1} cannot be empty or anything that is not an alphanumeric, dash, underscore, or dot. alias: <{2}>.".format(error_msg, ConnStrKeys.ALIAS, self.alias))
        logger().debug("kql_engine.py -_parse_common_connection_str - setting attributes- alias :  {0}".format(self.alias))

        # in case secret is missing, get it from user
        if len(secret_key_set) == 1:
            s = secret_key_set.pop()
            if s not in matched_keys_set or parsed_conn_kv[s] == "<{0}>".format(s):
                name = self.get_conn_name()
                parsed_conn_kv[s] = getpass.getpass(prompt="connection to {0} requires {1}, please enter {1}: ".format(name, s))
                matched_keys_set.add(s)

        logger().debug("kql_engine.py -_parse_common_connection_str - getting secret key from user :  {0}".format(secret_key_set))

        bind_url = []
        for key in conn_keys_list:
            if key not in self._EXCLUDE_FROM_URL_KEYS:
                bind_url.append("{0}('{1}')".format(key, parsed_conn_kv.get(key)))
        self.bind_url = "{0}://".format(uri_schema_name) + ".".join(bind_url)
        logger().debug("kql_engine.py -_parse_common_connection_str - setting attributes- bind_url :{0}".format(self.bind_url))

        return parsed_conn_kv

class KqlEngineError(Exception):
    """Generic error class."""
    pass

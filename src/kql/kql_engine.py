#-------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
#--------------------------------------------------------------------------
import re
import getpass
from kql.kql_proxy import KqlResponse
import functools


class KqlEngine(object):

    # Object constructor
    def __init__(self):
        self.bind_url = None
        self._parsed_conn = {}
        self.database_name = None
        self.cluster_name = None
        self.friendly_name = None
        self.client = None
        self.options = {}

        self.validated = None

    def __eq__(self, other):
        return self.bind_url and self.bind_url == other.bind_url

    def is_validated(self):
        return self.validated == True

    def set_validation_result(self, result):
        self.validated = result == True

    def get_database(self):
        if not self.database_name:
            raise KqlEngineError("Database is not defined.")
        return self.database_name

    def get_cluster(self):
        if not self.cluster_name:
            raise KqlEngineError("Cluster is not defined.")
        return self.cluster_name

    def get_conn_name(self):
        if self.database_name and self.cluster_name:
            return "{0}@{1}".format(self.friendly_name or self.database_name, self.cluster_name)
        else:
            raise KqlEngineError("Database and/or cluster is not defined.")

    def get_client(self):
        return self.client

    def client_execute(self, query, user_namespace=None, **kwargs):
        if query.strip():
            client = self.get_client()
            if not client:
                raise KqlEngineError("Client is not defined.")
            return  client.execute(self.get_database(), query, accept_partial_results=False, timeout=None)

    def execute(self, query, user_namespace=None, **kwargs):
        if query.strip():
            response = self.client_execute(query, user_namespace, **kwargs)
            # print(response.json_response)
            return KqlResponse(response, **kwargs)

    def validate(self, **kwargs):
        client = self.get_client()
        if not client:
            raise KqlEngineError("Client is not defined.")
        query = "range c from 1 to 10 step 1 | count"
        response = client.execute(self.get_database(), query, accept_partial_results=False, timeout=None)
        # print(response.json_response)
        table = KqlResponse(response, **kwargs).tables[0]
        if table.rowcount() != 1 or table.colcount() != 1 or [r for r in table.fetchall()][0][0] != 10:
            raise KqlEngineError("Client failed to validate connection.")

    _SECRET_KEYS = {"clientsecret", "appkey", "password", "certificate_thumbprint"}
    _NOT_INHERITABLE_KEYS = {"appkey", "name"}
    _OPTIONAL_KEYS = {"tenant", "name"}
    _INHERITABLE_KEYS = {"cluster", "tenant"}
    _EXCLUDE_FROM_URL_KEYS = {"database", "name"}
    _SHOULD_BE_NULL_KEYS = {"code"}

    _ALL_VALID_COMBINATIONS = [
            ["tenant", "code", "workspace", "name"],
            ["tenant", "clientid", "clientsecret", "workspace", "name"],
            ["workspace", "appkey", "name"], # only for demo, if workspace = "DEMO_WORKSPACE"

            ["tenant", "code", "appid", "name"],
            ["tenant", "clientid", "clientsecret", "appid", "name"],
            ["appid", "appkey", "name"],

            ["tenant", "code", "cluster", "database", "name"],
            ["tenant", "username", "password", "cluster", "database", "name"],
            ["tenant", "clientid", "clientsecret", "cluster", "database", "name"],
            ["tenant", "clientid", "certificate", "certificate_thumbprint", "cluster", "database", "name"],
        ]
    _ALL_KEYS = set()
    for c in _ALL_VALID_COMBINATIONS:
        _ALL_KEYS.update(set(c))

    def _parse_common_connection_str(self, conn_str: str, current, schema:str, mandatory_key:str, not_in_url_key=None):
        # get key/values in connection string
        self._parsed_conn = self._parse_and_get_connection_keys(conn_str, schema)
        matched_keys_set = set(self._parsed_conn.keys())

        # check for unknown keys
        unknonw_keys_set = matched_keys_set.difference(self._ALL_KEYS)
        if len(unknonw_keys_set) > 0:
            raise KqlEngineError("invalid connection string, detected unknown keys: {0}.".format(unknonw_keys_set))

        # check that mandatory key in matched set
        if mandatory_key not in matched_keys_set:
            raise KqlEngineError("invalid connection strin, mandatory key {0} is missing.".format(mandatory_key))

        # find a valid combination for the set
        valid_combinations = [c for c in self._ALL_VALID_COMBINATIONS if matched_keys_set.issubset(c)]
        # in case of ambiguity, assume it is based on current connection, resolve by copying missing values from current
        if len(valid_combinations) > 1:
            if current is not None:
                if self._parsed_conn.get("tenant") is None or self._parsed_conn.get("tenant") == current._parsed_conn.get("tenant"):
                    for k,v in current._parsed_conn.items():
                        if k not in matched_keys_set and k not in self._NOT_INHERITABLE_KEYS:
                            self._parsed_conn[k] = v
                            matched_keys_set.add(k)
        valid_combinations = [c for c in valid_combinations if matched_keys_set.issubset(c)]

        # only one combination can be accepted
        if len(valid_combinations) == 0:
            raise KqlEngineError('invalid connection string, not a valid keys set, missing keys.')

        conn_keys_list = None
        # if still too many choose the shortest
        if len(valid_combinations) > 1:
            for c in valid_combinations:
                if len(c) == 3:
                    conn_keys_list = c
        else:
            conn_keys_list = valid_combinations[0]
        
        if conn_keys_list is None:
            raise KqlEngineError('invalid connection string, not a valid keys set, missing keys.')

        conn_keys_set = set(conn_keys_list)

        # in case inheritable fields are missing inherit from current if exist
        inherit_keys_set = self._INHERITABLE_KEYS.intersection(conn_keys_set).difference(matched_keys_set)
        if len(inherit_keys_set) > 1:
            if current is not None:
                for k in inherit_keys_set:
                    v = current._parsed_conn.get(k)
                    if v is not None:
                        self._parsed_conn[k] = v
                        matched_keys_set.add(k)

        # make sure that all required keys are in set
        secret_key_set = self._SECRET_KEYS.intersection(conn_keys_set)
        missing_set = conn_keys_set.difference(matched_keys_set).difference(secret_key_set).difference(self._OPTIONAL_KEYS)
        if len(missing_set) > 0:
            raise KqlEngineError('invalid connection string, missing {0}.'.format(missing_set))

        # make sure that all required keys are with proper value
        for key in matched_keys_set: #.difference(secret_key_set).difference(self._SHOULD_BE_NULL_KEYS):
            if key in self._SHOULD_BE_NULL_KEYS:
                if self._parsed_conn[key] != "<{0}>".format(key):
                    raise KqlEngineError('invalid connection string, key {0} must be empty.'.format(key))
            elif key not in self._SECRET_KEYS:
                if self._parsed_conn[key] == "<{0}>".format(key):
                    raise KqlEngineError('invalid connection string, key {0} cannot be empty or set to <{1}>.'.format(key, key))

        # in case secret is missing, get it from user
        if len(secret_key_set) == 1:
            s = secret_key_set.pop()
            if s not in matched_keys_set or self._parsed_conn[s] == "<{0}>".format(s):
                self._parsed_conn[s] = getpass.getpass(prompt="please enter {0}: ".format(s))
                matched_keys_set.add(s)

        # set attribuets
        self.cluster_name = self._parsed_conn.get("cluster") or schema
        self.database_name = self._parsed_conn.get(mandatory_key)
        self.friendly_name = self._parsed_conn.get("name")
        bind_url = []
        for key in conn_keys_list:
            if key not in self._EXCLUDE_FROM_URL_KEYS:
                bind_url.append("{0}('{1}')".format(key, self._parsed_conn.get(key)))
        self.bind_url = "{0}://".format(schema) + '.'.join(bind_url)

    def _parse_and_get_connection_keys(self, conn_str:str, schema:str, extra_delimiter=".", lp_char="(", rp_char=")"):
        prefix = "{0}://".format(schema)
        if not conn_str.startswith(prefix):
            raise KqlEngineError('invalid connection string, must be prefixed by "<schema>://", supported schamas: "kusto", "loganalytics", "appinsights" and "cache"')

        matched_kv = {}
        rest = conn_str[len(prefix):].strip()
        delimiter_required = False
        while len(rest) > 0:
            lp_idx = rest.find(lp_char)
            if lp_idx < 0: 
                break
            key = rest[:lp_idx].strip()
            rest = rest[lp_idx+1:].strip()
            rp_idx = rest.find(rp_char)
            if rp_idx < 0: 
                if extra_delimiter is not None:
                    raise KqlEngineError("invalid connection string, missing right parethesis.")
                else:
                    val = rest
                    rest = ""
            else:
                val = rest[:rp_idx].strip()
                rest = rest[rp_idx+1:].strip()
            if extra_delimiter is not None:
                if key.startswith(extra_delimiter):
                    key = key[1:].strip()
                elif delimiter_required:
                    raise KqlEngineError("invalid connection string, missing delimiter.")
                delimiter_required = True
            if val == '':
                val = "<{0}>".format(key)
            elif val.startswith("'"):
                if len(val) >= 2 and val.endswith("'"):
                    val = val[1:-1]
                else:
                    raise KqlEngineError('invalid connection string.')
            elif val.startswith('"'):
                if len(val) >= 2 and val.endswith('"'):
                    val = val[1:-1]
                else:
                    raise KqlEngineError('invalid connection string.')
            elif val.startswith('"""'):
                if len(val) >= 6 and val.endswith('"""'):
                    val = val[3:-3]
                else:
                    raise KqlEngineError('invalid connection string.')
            matched_kv[key] = val
        return matched_kv



    def _validate_connection_delimiter(self, require_delimiter, delimiter):
        # delimiter '.' should separate between tokens
        if len(delimiter) > 0:
            if delimiter.strip() != ".":
                raise KqlEngineError("Invalid connection string.")
        elif require_delimiter:
            raise KqlEngineError("Invalid connection string.")

class KqlEngineError(Exception):
    """Generic error class."""

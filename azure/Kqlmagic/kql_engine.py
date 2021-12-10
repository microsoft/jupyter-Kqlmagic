# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

from typing import Any, List, Dict, Union, Tuple
import re
import itertools
import getpass


from .engine import Engine
from .kql_response import KqlQueryResponse, KqlSchemaResponse
from .kql_proxy import KqlResponse
from .kql_client import KqlClient
from .constants import ConnStrKeys, Schema
from .exceptions import KqlEngineError
from .my_utils import get_valid_name, adjust_path
from .parser import Parser
from .log import logger


_FQN_KUSTO_CLUSTER_PATTERN        = re.compile(r"(http(s?)\:\/\/)?(?P<cname>.*)\.kusto\.(windows\.net|chinacloudapi.cn|cloudapi.de|usgovcloudapi.net)$")
_FQN_DRAFT_PROXY_CLUSTER_PATTERN  = re.compile(r"http(s?)\:\/\/ade\.(int\.)?(?P<io>(applicationinsights|loganalytics))\.io\/subscriptions\/(?P<subscription>.*)$")
_FQN_DRAFT_PROXY_CLUSTER_PATTERN2 = re.compile(r"http(s?)\:\/\/adx\.(int\.)?(?P<io>(monitor))\.azure\.(com|us|cn)\/subscriptions\/(?P<subscription>.*)$")
_FQN_ARIA_KUSTO_CLUSTER_PATTERN   = re.compile(r"http(s?)\:\/\/kusto\.aria\.microsoft\.com$")


class KqlEngine(Engine):

    _ALT_URI_SCHEMA_NAMES:List[str] = []
    _RESERVED_CLUSTER_NAME:str = None
    _URI_SCHEMA_NAME:str = None
    _MANDATORY_KEY:str = None
    _VALID_KEYS_COMBINATIONS:List[List[str]] = []


    # Object constructor
    def __init__(self):
        super(KqlEngine, self).__init__()
        self.bind_url = None
        self._parsed_conn = {}
        self.database_name = None
        self.client_database_name = None
        self.database_friendly_name = None
        self.cluster_name = None
        self.cluster_friendly_name = None
        self.alias = None
        self.client = None
        self.options = {}

        self.validated = None
        self.conn_name = None
        self.id = None


    @classmethod
    def get_alt_uri_schema_names(cls)->List[str]:
        return cls._ALT_URI_SCHEMA_NAMES 


    @classmethod
    def get_reserved_cluster_name(cls)->str:
        return cls._RESERVED_CLUSTER_NAME


    @classmethod
    def get_mandatory_key(cls)->str:
        return cls._MANDATORY_KEY


    @classmethod
    def get_valid_keys_combinations(cls)->List[List[str]]:
        return cls._VALID_KEYS_COMBINATIONS


    @classmethod
    def get_uri_schema_name(cls):
        return cls._URI_SCHEMA_NAME


    def obfuscate_parsed_conn(self)->Dict[str,str]:
        parse_conn_copy = {**self._parsed_conn}
        for p in parse_conn_copy:
            if p in [ConnStrKeys.PASSWORD, ConnStrKeys.CLIENTSECRET, ConnStrKeys.CERTIFICATE, ConnStrKeys.APPKEY]:
                parse_conn_copy[p] = "*****"
        return parse_conn_copy


    # collect datails, in case bug report will be generated
    def get_details(self)->Dict[str,Any]:
        details =  {
            "cluster_name": self.cluster_name,
            "database_name": self.database_name,
            "parsed_conn": self.obfuscate_parsed_conn(),
        }
        client = self.get_client()
        if client and client._aad_helper:
            details["auth"] = client._aad_helper.get_details()
        return details


    def __eq__(self, other)->bool:
        return self.get_id() == other.get_id()


    def get_alias(self)->str:
        return self.alias


    def get_database_name(self)->str:
        if not self.database_name:
            raise KqlEngineError("Database is not defined.")
        return self.database_name


    def get_client_database_name(self)->str:
        name = self.client_database_name or self.database_name
        if name is None:
            raise KqlEngineError("Database is not defined.")
        return name


    def get_cluster_name(self)->str:
        if not self.cluster_name:
            raise KqlEngineError("Cluster is not defined.")
        return self.cluster_name


    def get_cluster_friendly_name(self)->str:
        if not self.cluster_friendly_name:
            raise KqlEngineError("Cluster friendly name is not defined.")
        return self.cluster_friendly_name


    def get_database_friendly_name(self)->str:
        if not self.database_friendly_name:
            raise KqlEngineError("Database friendly name is not defined.")
        return self.database_friendly_name


    def get_id(self)->str:
        if self.id is None:
            if self.cluster_friendly_name and self.database_friendly_name:
                self.id = f"{self.database_friendly_name}@{self.cluster_friendly_name}"
            else:
                raise KqlEngineError("Database and/or cluster is not defined.")
        return self.id



    def get_conn_name(self)->str:
        if self.conn_name is None:
            if self.alias and self.cluster_friendly_name and self.database_friendly_name:
                self.conn_name = f"{self.alias}@{self.cluster_friendly_name}"
            else:
                raise KqlEngineError("Database and/or cluster is not defined.")
        return self.conn_name


    def get_deep_link(self, query:str, options:Dict[str,Any]=None)->str:
        options = options or {}
        return None


    def createDatabaseFriendlyName(self, dname:str)->str:
        return get_valid_name(dname)


    def createClusterFriendlyName(self, cname:str)->str:
        name = cname.lower()
        match = _FQN_KUSTO_CLUSTER_PATTERN.match(name)
        if match:
            return match.group("cname")

        name = name[:-1] if name[-1] == "/" else name

        match = _FQN_DRAFT_PROXY_CLUSTER_PATTERN.match(name) or _FQN_DRAFT_PROXY_CLUSTER_PATTERN2.match(name)
        if match:
            components = match.group("subscription").split("/")
            if match.group("io") == "monitor":
                if len(components) >= 5 and components[3] == "providers":
                    resource_name = "app" if components[4] == "microsoft.insights" else "workspace"
                else:
                    resource_name = "apps_or_workspace"
            else:
                resource_name = "app" if match.group("io") == "applicationinsights" else "workspace"
            name = f"{resource_name}s_in_subscription_{components[0]}"
            if len(components) >= 3:
                if components[1] == "resourcegroups":
                    name = f"{name}_resourcegroup_{components[2]}"
                    if len(components) >= 7:
                        name = f"{name}_{resource_name}_{components[6]}"
                else:
                    name = f"{name}_{components[1]}_{components[2]}"
        elif _FQN_ARIA_KUSTO_CLUSTER_PATTERN.match(name):
            name = Schema.ARIA
        else:
            if name.startswith("https://"):
                name = name[8:]
            elif name.startswith("http://"):
                name = name[7:]
            name = name.replace("/", "_").replace(".", "_").replace(":", "_").replace("-", "_")
        return name


    def get_client(self)->KqlClient:
        return self.client


    def client_execute(self, query:str, user_namespace:Dict[str,Any]=None, database:str=None, **options)->Union[KqlQueryResponse, KqlSchemaResponse]:
        if query.strip():
            client = self.get_client()
            if not client:
                raise KqlEngineError("Client is not defined.")
            return client.execute(database or self.get_client_database_name(), query, accept_partial_results=False, **options)


    def execute(self, query:str, user_namespace:Dict[str,Any]=None, database:str=None, **options)->KqlResponse:
        if query.strip():
            response = self.client_execute(query, user_namespace, database, **options)
            return KqlResponse(response, **options)


    def validate(self, **options)->None:
        if not self.options.get("validate_connection_string_done") and options.get("validate_connection_string"):
            query = "range c from 1 to 10 step 1 | count"
            kql_response = self.execute(query, **options)
            table = kql_response.tables[0]
            if table.rowcount() != 1 or table.colcount() != 1 or [r for r in table.fetchall()][0][0] != 10:
                raise KqlEngineError("Client failed to validate connection.")
            self.options["validate_connection_string_done"] = True
        

    def validate_database_name(self, **options)->None:
        if not self.options.get("validate_database_name_done"):
            try:
                database_name_list, databases_by_pretty_name = self._get_databases_by_pretty_name(**options)
                if self.database_name not in database_name_list:
                    if self.database_name in databases_by_pretty_name:
                        self.client_database_name = databases_by_pretty_name[self.database_name]
                    else:
                        raise KqlEngineError("Database wasn't found in cluster.")
            except NotImplementedError:
                pass
            except:
                raise

            self.options["validate_database_name_done"] = True


    def _get_databases_by_pretty_name(self, **options)->Tuple[List[str],Dict[str,str]]:
        raise NotImplementedError(f"_get_databases_by_pretty_name not implemented for this engine.")


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

    _SECRET_KEYS = {
        ConnStrKeys.CLIENTSECRET, 
        ConnStrKeys.APPKEY, 
        ConnStrKeys.PASSWORD, 
        ConnStrKeys.CERTIFICATE_THUMBPRINT
    }

    _NOT_INHERITABLE_KEYS = {
        ConnStrKeys.APPKEY,
        ConnStrKeys.ALIAS
    }

    _OPTIONAL_KEYS = {
        ConnStrKeys.TENANT, 
        ConnStrKeys.AAD_URL, 
        ConnStrKeys.DATA_SOURCE_URL, 
        ConnStrKeys.ALIAS, 
        ConnStrKeys.CLIENTID
    }

    _INHERITABLE_KEYS = {
        ConnStrKeys.CLUSTER, 
        ConnStrKeys.TENANT,
        ConnStrKeys.AAD_URL,
        ConnStrKeys.DATA_SOURCE_URL
    }

    _EXCLUDE_FROM_URL_KEYS = {
        ConnStrKeys.DATABASE,
        ConnStrKeys.ALIAS
    }
    
    _SHOULD_BE_NULL_KEYS = {
        ConnStrKeys.CODE,
        ConnStrKeys.ANONYMOUS
    }


    def _parse_connection_str(self, conn_str:str, user_ns:Dict[str,Any])->Dict[str,str]:
        # logger().debug(f"kql_engine.py - _parse_connection_str - params:  conn_str: {conn_str}, user_ns: {safe_str(user_ns)}")
        logger().debug(f"kql_engine.py - _parse_connection_str - params:  conn_str: {conn_str}")
        rest = conn_str[conn_str.find("://") + 3:].strip()
        # get key/values in connection string
        parsed_conn_kv = Parser.parse_and_get_kv_string(rest, user_ns)
        logger().debug(f"kql_engine.py - _parse_connection_str - parsed_conn_kv: {parsed_conn_kv} (return of Parser.parse_and_get_kv_string)")
        # In case certificate_pem_file was specified instead of certificate.
        pem_file_name = parsed_conn_kv.get(ConnStrKeys.CERTIFICATE_PEM_FILE)
        if pem_file_name is not None:
            pem_file_name = adjust_path(pem_file_name)
            with open(pem_file_name, "r") as pem_file:
                parsed_conn_kv[ConnStrKeys.CERTIFICATE] = pem_file.read()
                del parsed_conn_kv[ConnStrKeys.CERTIFICATE_PEM_FILE]
        return parsed_conn_kv


    def _check_for_unknown_keys(self, matched_keys_set:set, keys_combinations:List[List[str]])->None:
        # check for unknown keys
        all_keys = set(itertools.chain(*keys_combinations))
        unknonw_keys_set = matched_keys_set.difference(all_keys)
        if len(unknonw_keys_set) > 0:
            logger().debug(f"kql_engine.py - _check_for_unknown_keys - the following keys are unknow: {unknonw_keys_set}")
            raise ValueError(f"detected unknown keys: {unknonw_keys_set}")


    def _check_for_mandatory_key(self, matched_keys_set:set, mandatory_key:str)->None:
        # check that mandatory key in matched set
        if mandatory_key not in matched_keys_set:
            logger().debug(f"kql_engine.py - _check_for_mandatory_key - the following mandatory_key is missing: {mandatory_key}")
            raise KqlEngineError(f"mandatory key {mandatory_key} is missing")


    def _find_combination_set(self, current, matched_keys_set:set, parsed_conn_kv:Dict[str,str], keys_combinations:List[List[str]])->set:
        # find a valid combination for the set
        valid_combinations = [c for c in keys_combinations if matched_keys_set.issubset(c)]
        logger().debug(f"kql_engine.py - _find_combination - found these valid_combinations: {valid_combinations}")

        # in case of ambiguity, assume it is based on current connection, resolve by copying missing values from current
        if len(valid_combinations) > 1:
            failed = False
            saved_parsed_conn_kv = parsed_conn_kv.copy()
            saved_matched_keys_set = matched_keys_set.copy()
            if current is not None:
                inherited = False
                for k, v in current._parsed_conn.items():
                    if k not in matched_keys_set and k not in self._NOT_INHERITABLE_KEYS:
                        parsed_conn_kv[k] = v
                        matched_keys_set.add(k)
                        inherited = True
                        
                if inherited:
                    for k in self._CREDENTIAL_KEYS.intersection(matched_keys_set):
                        if parsed_conn_kv[k] != current._parsed_conn.get(k):
                            failed = True
                            # raise KqlEngineError("missing keys.")

                    if failed:
                        parsed_conn_kv = saved_parsed_conn_kv
                        matched_keys_set= saved_matched_keys_set

            valid_combinations = [c for c in valid_combinations if matched_keys_set.issubset(c)]


        logger().debug(f"kql_engine.py - _find_combination - inherited from current and now valid_combinations is: {valid_combinations}")

        # only one combination can be accepted
        if len(valid_combinations) == 0:
            logger().debug(f"kql_engine.py - _find_combination - valid_combinations is empty: {valid_combinations}")
            raise KqlEngineError("not a valid keys set, missing keys")

        combination_keys_list = None
        # if still too many choose the shortest
        if len(valid_combinations) > 1:
            combination_keys_list =  min(valid_combinations, key=len)
        else:
            combination_keys_list = valid_combinations[0]
        logger().debug(f"kql_engine.py - _find_combination - chose combination_keys_list - shortest out of valid_combinations: {combination_keys_list}")

        if combination_keys_list is None:
            raise KqlEngineError("not a valid keys set, missing keys")

        return set(combination_keys_list)


    def _inherit_keys(self, current, matched_keys_set:set, parsed_conn_kv:Dict[str,str], keys_set:set)->None:
        # in case inheritable fields are missing inherit from current if exist
        inherit_keys_set = self._INHERITABLE_KEYS.intersection(keys_set).difference(matched_keys_set)
        if len(inherit_keys_set) > 1:
            if current is not None:
                for k in inherit_keys_set:
                    v = current._parsed_conn.get(k)
                    if v is not None:
                        parsed_conn_kv[k] = v
                        matched_keys_set.add(k)
        logger().debug(f"kql_engine.py - _inherit_keys - add inherited to combination_keys_set and now inherit_keys_set: {inherit_keys_set}")


    def _check_for_required_keys(self, matched_keys_set:set, keys_set:set)->None:
        # make sure that all required keys are in set
        secret_key_set = self._SECRET_KEYS.intersection(keys_set)
        missing_set = keys_set.difference(matched_keys_set).difference(secret_key_set).difference(self._OPTIONAL_KEYS)
        if len(missing_set) > 0:
            logger().debug(f"kql_engine.py - _check_for_required_keys - the following required key is missing : {missing_set}")
            raise KqlEngineError(f"missing {missing_set}")


    def _check_for_special_combination(self, matched_keys_set:set, keys_set:set)->None:
        # special case although tenant in _OPTIONAL_KEYS
        if ConnStrKeys.TENANT not in matched_keys_set and ConnStrKeys.CLIENTID in keys_set:
            logger().debug("kql_engine.py - _check_for_special_combination - special case- if clientId exists but tenant doesnt")
            raise KqlEngineError("missing tenant key/value")


    def _check_for_restricted_values(self, matched_keys_set:set, parsed_conn_kv:Dict[str,str])->None:
        # make sure that all required keys are with proper value
        for key in matched_keys_set:  # .difference(secret_key_set).difference(self._SHOULD_BE_NULL_KEYS):
            if key in self._SHOULD_BE_NULL_KEYS:
                if parsed_conn_kv[key] != "":
                    raise KqlEngineError(f"key {key} must be empty")
            elif key not in self._SECRET_KEYS:
                if parsed_conn_kv[key] == f"<{key}>" or parsed_conn_kv[key] == "":
                    raise KqlEngineError(f"key {key} cannot be empty or set to <{key}>")
        logger().debug(f"kql_engine.py - _check_for_restricted_values - make sure that all required keys are with proper value: {matched_keys_set}")


    def _set_and_check_for_cluster_name(self, parsed_conn_kv:Dict[str,str], uri_schema_name:str)->None:
        cluster_name = parsed_conn_kv.get(ConnStrKeys.CLUSTER) or uri_schema_name
        if cluster_name is not None:
            if len(cluster_name) < 1:
                raise KqlEngineError(f"key {ConnStrKeys.CLUSTER} cannot be empty")
            self.cluster_friendly_name = self.createClusterFriendlyName(cluster_name)
        self.cluster_name = cluster_name
        logger().debug(f"kql_engine.py -_set_and_check_for_cluster_name - setting attributes - cluster_name: {self.cluster_name}")
        logger().debug(f"kql_engine.py -_set_and_check_for_cluster_name - setting attributes - cluster_friendly_name: {self.cluster_friendly_name}")


    def _set_and_check_for_database_name(self, parsed_conn_kv:Dict[str,str], mandatory_key:str)->None:
        database_name = parsed_conn_kv.get(mandatory_key)
        if database_name is not None:
            if len(database_name) < 1:
                raise KqlEngineError(f"key {mandatory_key} cannot be empty")
            self.database_friendly_name = self.createDatabaseFriendlyName(database_name)
        self.database_name = database_name
        logger().debug(f"kql_engine.py - _set_and_check_for_database_name - setting attributes - database_name: {self.database_name}")
        logger().debug(f"kql_engine.py - _set_and_check_for_database_name - setting attributes - database_friendly_name: {self.database_friendly_name}")


    def _set_and_check_for_alias(self, parsed_conn_kv:Dict[str,str], friendly_name:str)->None:
        alias = parsed_conn_kv.get(ConnStrKeys.ALIAS) or friendly_name
        if alias is not None:
            if len(alias) < 1 or alias != get_valid_name(alias):
                raise KqlEngineError(f"key {ConnStrKeys.ALIAS} cannot be empty or anything that is not an alphanumeric, dash, underscore, or dot. alias: <{alias}>")
        self.alias = alias
        logger().debug(f"kql_engine.py - _set_and_check_for_alias - setting attributes - alias: {self.alias}")


    def _retreive_and_set_secret(self, matched_keys_set:set, parsed_conn_kv:Dict[str,str], keys_set:set)->None:
        # in case secret is missing, get it from user
        secret_key_set = self._SECRET_KEYS.intersection(keys_set)
        if len(secret_key_set) == 1:
            s = secret_key_set.pop()
            if s not in matched_keys_set or parsed_conn_kv[s] == f"<{s}>":
                name = self.get_conn_name()
                parsed_conn_kv[s] = getpass.getpass(prompt=f"connection to {name} requires {s}, please enter {s}: ")
                matched_keys_set.add(s)
                logger().debug(f"kql_engine.py - _retreive_and_set_secret - getting secret key from user: {s}")


    def _create_and_set_bind_url(self, parsed_conn_kv:Dict[str,str], keys_set:set, uri_schema_name:str)->None:
        items = []
        for key in keys_set:
            if key not in self._EXCLUDE_FROM_URL_KEYS:
                items.append(f"{key}('{parsed_conn_kv.get(key)}')")
        self.bind_url = f"{uri_schema_name}://{'.'.join(items)}"
        logger().debug(f"kql_engine.py - _create_and_set_bind_url - setting attributes - bind_url:{self.bind_url}")


    def _parse_common_connection_str(self, conn_str:str, current, uri_schema_name:str, mandatory_key:str, keys_combinations:List[List[str]], user_ns:Dict[str,Any])->Dict[str,str]:
        # logger().debug(f"kql_engine.py -_parse_common_connection_str - params:  conn_str: {conn_str}; current: {current}, uri_schema_name: {uri_schema_name};mandatory_key: {mandatory_key}, valid_keys_combinations: {keys_combinations}, user_ns: {safe_str(user_ns)}")
        logger().debug(f"kql_engine.py -_parse_common_connection_str - params:  conn_str: {conn_str}; current: {current}, uri_schema_name: {uri_schema_name};mandatory_key: {mandatory_key}, valid_keys_combinations: {keys_combinations}")

        try:
            parsed_conn_kv = self._parse_connection_str(conn_str, user_ns)

            matched_keys_set = set(parsed_conn_kv.keys())
            logger().debug(f"kql_engine.py -_parse_common_connection_str - matched_keys_set: {matched_keys_set}")

            self._check_for_unknown_keys(matched_keys_set, keys_combinations)
            self._check_for_mandatory_key(matched_keys_set, mandatory_key)
            keys_set = self._find_combination_set(current, matched_keys_set, parsed_conn_kv, keys_combinations)

            self._inherit_keys(current, matched_keys_set, parsed_conn_kv, keys_set)

            self._check_for_required_keys(matched_keys_set, keys_set)
            self._check_for_special_combination(matched_keys_set, keys_set)
            self._check_for_restricted_values(matched_keys_set, parsed_conn_kv)

            self._set_and_check_for_cluster_name(parsed_conn_kv, uri_schema_name)
            self._set_and_check_for_database_name(parsed_conn_kv, mandatory_key)
            self._set_and_check_for_alias(parsed_conn_kv, self.database_friendly_name)

            self._retreive_and_set_secret(matched_keys_set, parsed_conn_kv, keys_set)

            self._create_and_set_bind_url(parsed_conn_kv, keys_set, uri_schema_name)

            return parsed_conn_kv
        except Exception as exception:
            raise KqlEngineError(f"invalid connection string: {conn_str}, {exception}")

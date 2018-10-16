#-------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
#--------------------------------------------------------------------------
import getpass
from Kqlmagic.kql_proxy import KqlResponse
import functools


class KqlEngine(object):

    # Object constructor
    def __init__(self):
        self.bind_url = None
        self._parsed_conn = {}
        self.database_name = None
        self.cluster_name = None
        self.alias = None
        self.client = None
        self.options = {}

        self.validated = None

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

    def get_conn_name(self):
        if self.database_name and self.cluster_name:
            return "{0}@{1}".format(self.alias or self.database_name, self.cluster_name)
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

    _CREDENTIAL_KEYS = {"tenant", "username", "clienid", "certificate", "clientsecret", "appkey", "password", "certificate_thumbprint"}
    _SECRET_KEYS = {"clientsecret", "appkey", "password", "certificate_thumbprint"}
    _NOT_INHERITABLE_KEYS = {"appkey", "alias"}
    _OPTIONAL_KEYS = {"tenant", "alias"}
    _INHERITABLE_KEYS = {"cluster", "tenant"}
    _EXCLUDE_FROM_URL_KEYS = {"database", "alias"}
    _SHOULD_BE_NULL_KEYS = {"code"}

    def _parse_common_connection_str(self, conn_str: str, current, uri_schema_name, mandatory_key:str, alt_names:list, all_keys:list, valid_keys_combinations:list):
        # get key/values in connection string
        parsed_conn_kv = self._parse_and_get_connection_keys(conn_str, uri_schema_name, alt_names)
        matched_keys_set = set(parsed_conn_kv.keys())

        # check for unknown keys
        unknonw_keys_set = matched_keys_set.difference(all_keys)
        if len(unknonw_keys_set) > 0:
            raise KqlEngineError("invalid connection string, detected unknown keys: {0}.".format(unknonw_keys_set))

        # check that mandatory key in matched set
        if mandatory_key not in matched_keys_set:
            raise KqlEngineError("invalid connection strin, mandatory key {0} is missing.".format(mandatory_key))

        # find a valid combination for the set
        valid_combinations = [c for c in valid_keys_combinations if matched_keys_set.issubset(c)]
        # in case of ambiguity, assume it is based on current connection, resolve by copying missing values from current
        if len(valid_combinations) > 1:
            if current is not None:
                for k,v in current._parsed_conn.items():
                    if k not in matched_keys_set and k not in self._NOT_INHERITABLE_KEYS:
                        parsed_conn_kv[k] = v
                        matched_keys_set.add(k)
                for k in self._CREDENTIAL_KEYS.intersection(matched_keys_set):
                    if parsed_conn_kv[k] != current._parsed_conn.get(k):
                        raise KqlEngineError('invalid connection string, not a valid keys set, missing keys.')
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
                        parsed_conn_kv[k] = v
                        matched_keys_set.add(k)

        # make sure that all required keys are in set
        secret_key_set = self._SECRET_KEYS.intersection(conn_keys_set)
        missing_set = conn_keys_set.difference(matched_keys_set).difference(secret_key_set).difference(self._OPTIONAL_KEYS)
        if len(missing_set) > 0:
            raise KqlEngineError('invalid connection string, missing {0}.'.format(missing_set))
        # special case although tenant in _OPTIONAL_KEYS
        if parsed_conn_kv.get("tenant") is None and "clienid" in conn_keys_set:
            raise KqlEngineError('invalid connection string, missing tenant key/value.')

        # make sure that all required keys are with proper value
        for key in matched_keys_set: #.difference(secret_key_set).difference(self._SHOULD_BE_NULL_KEYS):
            if key in self._SHOULD_BE_NULL_KEYS:
                if parsed_conn_kv[key] != "<{0}>".format(key):
                    raise KqlEngineError('invalid connection string, key {0} must be empty.'.format(key))
            elif key not in self._SECRET_KEYS:
                if parsed_conn_kv[key] == "<{0}>".format(key):
                    raise KqlEngineError('invalid connection string, key {0} cannot be empty or set to <{1}>.'.format(key, key))

        # in case secret is missing, get it from user
        if len(secret_key_set) == 1:
            s = secret_key_set.pop()
            if s not in matched_keys_set or parsed_conn_kv[s] == "<{0}>".format(s):
                parsed_conn_kv[s] = getpass.getpass(prompt="please enter {0}: ".format(s))
                matched_keys_set.add(s)

        # set attribuets
        self.cluster_name = parsed_conn_kv.get("cluster") or uri_schema_name
        self.database_name = parsed_conn_kv.get(mandatory_key)
        self.alias = parsed_conn_kv.get("alias")
        bind_url = []
        for key in conn_keys_list:
            if key not in self._EXCLUDE_FROM_URL_KEYS:
                bind_url.append("{0}('{1}')".format(key, parsed_conn_kv.get(key)))
        self.bind_url = "{0}://".format(uri_schema_name) + '.'.join(bind_url)
        return parsed_conn_kv

    def _parse_and_get_connection_keys(self, conn_str:str, uri_schema_name, alt_names:list):
        prefix = "{0}://".format(uri_schema_name)
        for n in alt_names:
            if conn_str.startswith(n):
                prefix = "{0}://".format(n)

        if not conn_str.startswith(prefix):
            raise KqlEngineError('invalid connection string, must be prefixed by a valid "<uri schema name>://"')

        matched_kv = {}
        rest = conn_str[len(prefix):].strip()
        delimiter_required = False
        lp_idx = rest.find('(')
        eq_idx = rest.find('=')
        sc_idx = rest.find(';')
        l_char = '(' if eq_idx < 0 and sc_idx < 0 else '=' if lp_idx < 0 else '(' if lp_idx < eq_idx and lp_idx < sc_idx else '='
        r_char = ')' if l_char == '(' else ';'
        extra_delimiter = None if r_char == ';' else '.'

        while len(rest) > 0:
            l_idx = rest.find(l_char)
            r_idx = rest.find(r_char)
            if l_idx < 0:
                if l_char == '(':
                    # string ends with delimiter
                    if extra_delimiter is not None and extra_delimiter == rest:
                        break
                    else:
                        raise KqlEngineError("invalid connection string, missing left parethesis.")
                # key only at end of string
                elif r_idx < 0:
                    key = rest
                    val = ''
                    rest = ''
                # key only 
                else:
                    key = rest[:r_idx].strip()
                    val = ''
                    rest = rest[r_idx+1:].strip()
            # key only
            elif r_idx >= 0 and r_idx < l_idx:
                if l_char == '(':
                    raise KqlEngineError("invalid connection string, missing left parethesis.")
                else:
                    key = rest[:r_idx].strip()
                    val = ''
                    rest = rest[r_idx+1:].strip()
            # key and value
            else:
                key = rest[:l_idx].strip()
                rest = rest[l_idx+1:].strip()
                r_idx = rest.find(r_char)
                if r_idx < 0:
                    if l_char == '(':
                        raise KqlEngineError("invalid connection string, missing right parethesis.")
                    else:
                        val = rest
                        rest = ""
                else:
                    val = rest[:r_idx].strip()
                    rest = rest[r_idx+1:].strip()
                if extra_delimiter is not None:
                    if key.startswith(extra_delimiter):
                        key = key[1:].strip()
                    elif delimiter_required:
                        raise KqlEngineError("invalid connection string, missing delimiter.")
                    delimiter_required = True

            # key exist
            if len(key) > 0:
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
            # no key but value exist
            elif len(val) > 0:
                raise KqlEngineError("invalid connection string, missing key.")
            # no key, no value in parenthesis mode
            elif l_char == '(':
                raise KqlEngineError("invalid connection string, missing key.")

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

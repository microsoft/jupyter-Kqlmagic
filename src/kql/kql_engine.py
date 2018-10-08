#-------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
#--------------------------------------------------------------------------
import re
import getpass
from kql.kql_proxy import KqlResponse


class KqlEngine(object):

    # Object constructor
    def __init__(self):
        self.bind_url = None
        self._parsed_conn = {}
        self.database_name = None
        self.cluster_name = None
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
            return "{0}@{1}".format(self.database_name, self.cluster_name)
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

    def _parse_common_connection_str(self, conn_str: str, current, schema:str, keys:list, mandatory_key:str, not_in_url_key=None):
        prefix_matched = False
        conn_str_rest = None

        # parse connection string prefix
        pattern = re.compile(r"^{0}://(?P<conn_str_rest>.*)$".format(schema))
        match = pattern.search(conn_str.strip())
        if not match:
            raise KqlEngineError('Invalid connection string, must be prefixed by "kusto://"')
        conn_str_rest = match.group("conn_str_rest")

        # parse all keys sequentially
        for key in keys:
            pattern = re.compile("^(?P<delimiter>.?){0}\\((?P<{1}>.*?)\\)(?P<conn_str_rest>.*)$".format(key, key))
            match = pattern.search(conn_str_rest)
            if match:
                self._validate_connection_delimiter(prefix_matched, match.group("delimiter"))
                conn_str_rest = match.group("conn_str_rest")
                prefix_matched = True
                self._parsed_conn[key] = match.group(key).strip()[1:-1] if key != "code" else "<code>"

        # at least one key must be matched, and we should have nothing more to parse
        if not prefix_matched or len(conn_str_rest) > 0:
            raise KqlEngineError("Invalid connection string.")

        # code cannot be followerd by clientsecret or username of password
        if self._parsed_conn.get("code") and (
            self._parsed_conn.get("clientsecret") or 
            self._parsed_conn.get("username") or 
            self._parsed_conn.get("password") or 
            self._parsed_conn.get("certificate") or 
            self._parsed_conn.get("certificate_thumbprint")
        ):
            raise KqlEngineError('Invalid connection string, code cannot be followed username or password or clientsecret or certificate or certificate_thumbprint.')

        # clientsecret can only follow clientid
        if self._parsed_conn.get("clientsecret") and self._parsed_conn.get("clientid") is None:
            raise KqlEngineError('Invalid connection string, clientsecret must be together with clientid.')

        # clientsecret cannot be followerd by user or certificate credentials
        if self._parsed_conn.get("clientsecret") and (
            self._parsed_conn.get("password") or 
            self._parsed_conn.get("username") or
            self._parsed_conn.get("certificate") or 
            self._parsed_conn.get("certificate_thumbprint")
            ):
            raise KqlEngineError('Invalid connection string, clientsecret cannot be followed username or password or certificate or certificate_thumbprint.')

        # password can only follow username
        if self._parsed_conn.get("password") and self._parsed_conn.get("username") is None:
            raise KqlEngineError('Invalid connection string, password must be together with username.')

        # certificate_thumbprint can only follow certificate
        if self._parsed_conn.get("certificate_thumbprint") and self._parsed_conn.get("certificate") is None:
            raise KqlEngineError('Invalid connection string, certificate_thumbprint must be together with certificate.')

        # database is mandatory
        if self._parsed_conn.get(mandatory_key) is None:
            raise KqlEngineError("{0} is not defined.".format(mandatory_key))

        if "cluster" in keys and self._parsed_conn.get("cluster") is None:
            if current is None  or current._parsed_conn.get("cluster") is None:
                raise KqlEngineError("Cluster is not defined.")
            self._parsed_conn["cluster"] = current._parsed_conn.get("cluster")

        # if authentication credential are missing, try to add them from current connection
        if (self._parsed_conn.get("username") is None and 
            self._parsed_conn.get("clientid") is None and 
            self._parsed_conn.get("certificate") is None and 
            self._parsed_conn.get("code") is None):
            if current is None:
                raise KqlEngineError("username/password NOR clientid/clientsecret NOR certificate/certificate_thumbprint NOR code() are defined.")
            for key in keys:
                self._parsed_conn[key] = self._parsed_conn.get(key) or current._parsed_conn.get(key)

        # if clientid and it is not code or username/password pattern, get clientsecret interactively
        if (
            self._parsed_conn.get("clientid")
            and self._parsed_conn.get("username") is None
            and self._parsed_conn.get("certificate") is None
            and self._parsed_conn.get("code") is None
            and (self._parsed_conn.get("clientsecret") is None or self._parsed_conn.get("clientsecret").lower() == "<clientsecret>")
        ):
            self._parsed_conn["clientsecret"] = getpass.getpass(prompt="please enter clientsecret: ")

        # if username and password is missing
        if self._parsed_conn.get("username") and (self._parsed_conn.get("password") is None or self._parsed_conn.get("password").lower() == "<password>"):
            self._parsed_conn["password"] = getpass.getpass(prompt="please enter password: ")

        # if certificate and certificate_thumbprint is missing
        if self._parsed_conn.get("certificate") and (self._parsed_conn.get("certificate_thumbprint") is None or self._parsed_conn.get("certificate_thumbprint").lower() == "<thumbprint>"):
            self._parsed_conn["certificate_thumbprint"] = getpass.getpass(prompt="please enter certificate thumbprint: ")

        if (self._parsed_conn.get("code") is None and 
            self._parsed_conn.get("clientsecret") is None and 
            self._parsed_conn.get("password") is None and
            self._parsed_conn.get("certificate_thumbprint") is None):
            raise KqlEngineError("credentials are not fully set.")

        self.cluster_name = self._parsed_conn.get("cluster") or schema
        self.database_name = self._parsed_conn.get(mandatory_key)
        bind_url = []
        for key in keys:
            if not_in_url_key is None or key != not_in_url_key:
                bind_url.append("{0}('{1}')".format(key, self._parsed_conn.get(key)))
        self.bind_url = "{0}://".format(schema) + '.'.join(bind_url)

    def _validate_connection_delimiter(self, require_delimiter, delimiter):
        # delimiter '.' should separate between tokens
        if require_delimiter:
            if delimiter != ".":
                raise KqlEngineError("Invalid connection string, missing or wrong delimiter")
        # delimiter '.' should not exsit before first token
        else:
            if len(delimiter) > 0:
                raise KqlEngineError("Invalid connection string.")

class KqlEngineError(Exception):
    """Generic error class."""

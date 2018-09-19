#-------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
#--------------------------------------------------------------------------

import re
import getpass

from kql.kql_engine import KqlEngine, KqlEngineError
from kql.kusto_client import Kusto_Client


class KustoEngine(KqlEngine):
    schema = "kusto://"

    @classmethod
    def tell_format(cls):
        return """
               kusto://username('username').password('password').cluster('clustername').database('databasename')
               kusto://cluster('clustername').database('databasename')
                     # Note: current username and password are attached
               kusto://database('databasename')
                     # Note: current username, password and cluster are attached
               kusto://username('username').password('password').cluster('clustername')
                     # Note: not enough for to submit a query, set current username, passsword and clustername, 
               kusto://username('username').password('password')
                     # Note: not enough for to submit a query, set current username and password 
               kusto://cluster('clustername')
                     # Note: not enough for to submit a query, set current clustername, current username and password are attached

               ## Note: if password is missing, user will be prompted to enter password"""

    # Object constructor
    def __init__(self, conn_str, current=None, conn_class=None):
        super().__init__()
        if isinstance(conn_str, list):
            self.conn_class = conn_class
            self.database_name = conn_str[0]
            self.cluster_name = conn_str[1]
            self.bind_url = "kusto://cluster('{0}').database('{1}')".format(self.cluster_name, self.database_name)
        else:
            # self.client_id = self.client_id or 'e07cf1fb-c6a6-4668-b21a-f74731afa19a'
            self.cluster_url_template = "https://{0}.kusto.windows.net"
            self._parsed_conn = {}
            self._parse_connection_str(conn_str, current)
            self._set_client()

    def _validate_connection_delimiter(self, require_delimiter, delimiter):
        # delimiter '.' should separate between tokens
        if require_delimiter:
            if delimiter != ".":
                raise KqlEngineError("Invalid connection string, missing or wrong delimiter")
        # delimiter '.' should not exsit before first token
        else:
            if len(delimiter) > 0:
                raise KqlEngineError("Invalid connection string.")

    def _parse_connection_str(self, conn_str: str, current):
        prefix_matched = False
        conn_str_rest = None

        # parse connection string prefix
        pattern = re.compile(r"^kusto://(?P<conn_str_rest>.*)$")
        match = pattern.search(conn_str.strip())
        if not match:
            raise KqlEngineError('Invalid connection string, must be prefixed by "kusto://"')
        conn_str_rest = match.group("conn_str_rest")

        # parse all tokens sequentially
        for token in ["tenant", "code", "clientid", "clientsecret", "username", "password", "cluster", "database"]:
            pattern = re.compile("^(?P<delimiter>.?){0}\\((?P<{1}>.*?)\\)(?P<conn_str_rest>.*)$".format(token, token))
            match = pattern.search(conn_str_rest)
            if match:
                self._validate_connection_delimiter(prefix_matched, match.group("delimiter"))
                conn_str_rest = match.group("conn_str_rest")
                prefix_matched = True
                self._parsed_conn[token] = match.group(token).strip()[1:-1] if token != "code" else "<code>"

        # at least one token must be matched, and we should have nothing more to parse
        if not prefix_matched or len(conn_str_rest) > 0:
            raise KqlEngineError("Invalid connection string.")

        # code cannot be followerd by clientsecret or username of password
        if self._parsed_conn.get("code") and (
            self._parsed_conn.get("clientsecret") or self._parsed_conn.get("password") or self._parsed_conn.get("username")
        ):
            raise KqlEngineError('Invalid connection string, code cannot be followed username or password or clientsecret."')

        # clientsecret can only follow clientid
        if self._parsed_conn.get("clientsecret") and not self._parsed_conn.get("clientid"):
            raise KqlEngineError('Invalid connection string, clientsecret must be prefixed by clientid."')

        # code cannot be followerd by clientsecret or username of password
        if self._parsed_conn.get("clientsecret") and (self._parsed_conn.get("password") or self._parsed_conn.get("username")):
            raise KqlEngineError('Invalid connection string, clientsecret cannot be followed username or password."')

        # password can only follow username
        if self._parsed_conn.get("password") and not self._parsed_conn.get("username"):
            raise KqlEngineError('Invalid connection string, password must be prefixed by username."')

        # database is mandatory
        if not self._parsed_conn.get("database"):
            raise KqlEngineError("database is not defined.")

        if not self._parsed_conn.get("cluster"):
            if not current:
                raise KqlEngineError("Cluster is not defined.")
            self._parsed_conn["cluster"] = current._parsed_conn.get("cluster")

        # if authentication credential are missing, try to add them from current connection
        if not (self._parsed_conn.get("username") or self._parsed_conn.get("clientid") or self._parsed_conn.get("code")):
            if not current:
                raise KqlEngineError("username/password NOR clientid/clientsecret NOR code() are not defined.")
            self._parsed_conn["clientid"] = current._parsed_conn.get("clientid")
            self._parsed_conn["username"] = current._parsed_conn.get("username")
            self._parsed_conn["password"] = current._parsed_conn.get("password")
            self._parsed_conn["clientsecret"] = current._parsed_conn.get("clientsecret")
            self._parsed_conn["code"] = current._parsed_conn.get("code")
            self._parsed_conn["tenant"] = current._parsed_conn.get("tenant")

        # if clientid and it is not code or username/password pattern, get clientsecret interactively
        if (
            self._parsed_conn.get("clientid")
            and not self._parsed_conn.get("username")
            and not self._parsed_conn.get("code")
            and (not self._parsed_conn.get("clientsecret") or self._parsed_conn.get("clientsecret").lower() == "<clientsecret>")
        ):
            self._parsed_conn["clientsecret"] = getpass.getpass(prompt="please enter clientsecret: ")

        # if username and password is missing
        if self._parsed_conn.get("username") and (not self._parsed_conn.get("password") or self._parsed_conn.get("password").lower() == "<password>"):
            self._parsed_conn["password"] = getpass.getpass(prompt="please enter password: ")

        if not (self._parsed_conn.get("code") or self._parsed_conn.get("clientsecret") or self._parsed_conn.get("password")):
            raise KqlEngineError("credentials are not set.")

        self.cluster_name = self._parsed_conn.get("cluster")
        self.database_name = self._parsed_conn.get("database")
        self.bind_url = "kusto://tenant('{0}').code('{1}').clientid('{2}').clientsecret('{3}').username('{4}').password('{5}').cluster('{6}')".format(
            self._parsed_conn.get("tenant"),
            self._parsed_conn.get("code"),
            self._parsed_conn.get("clientid"),
            self._parsed_conn.get("clientsecret"),
            self._parsed_conn.get("username"),
            self._parsed_conn.get("password"),
            self._parsed_conn.get("cluster"),
        )

    #            self._parsed_conn.get('database'))

    def _set_client(self):
        self.client = Kusto_Client(
            kusto_cluster=self.cluster_url_template.format(self._parsed_conn.get("cluster")),
            client_id=self._parsed_conn.get("clientid"),
            client_secret=self._parsed_conn.get("clientsecret"),
            username=self._parsed_conn.get("username"),
            password=self._parsed_conn.get("password"),
            certificate=self._parsed_conn.get("certificate"),
            certificate_thumbprint=self._parsed_conn.get("certificate_thumbprint"),
            authority=self._parsed_conn.get("tenant"),
        )

    def get_client(self):
        if self.client is None:
            cluster_connection = self.conn_class.get_connection_by_name("@" + self.cluster_name)
            if cluster_connection is None:
                raise KqlEngineError("connection to cluster not set.")
            return cluster_connection.get_client()
        else:
            return self.client

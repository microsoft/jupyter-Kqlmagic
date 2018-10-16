#-------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
#--------------------------------------------------------------------------

from Kqlmagic.kql_engine import KqlEngine, KqlEngineError
from Kqlmagic.kusto_client import Kusto_Client


class KustoEngine(KqlEngine):
    _URI_SCHEMA_NAME = "kusto"
    _ALT_URI_SCHEMA_NAMES = [_URI_SCHEMA_NAME, "adx", "ade", "azuredataexplorer", "azure_data_explorer"]
    _MANDATORY_KEY = "database"
    _VALID_KEYS_COMBINATIONS = [
            ["tenant", "code", "cluster", "database", "alias"],
            ["tenant", "username", "password", "cluster", "database", "alias"],
            ["tenant", "clientid", "clientsecret", "cluster", "database", "alias"],
            ["tenant", "clientid", "certificate", "certificate_thumbprint", "cluster", "database", "alias"],
    ]
    _ALL_KEYS = set()
    for c in _VALID_KEYS_COMBINATIONS:
        _ALL_KEYS.update(set(c))

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
        if isinstance(conn_str, dict):
            self.conn_class = conn_class
            self.database_name = conn_str.get("database_name")
            self.cluster_name = conn_str.get("cluster_name")
            self.alias = conn_str.get("alias")
            self.bind_url = "{0}://cluster('{1}').database('{2}')".format(self._URI_SCHEMA_NAME, self.cluster_name, self.database_name)
        else:
            # self.client_id = self.client_id or 'e07cf1fb-c6a6-4668-b21a-f74731afa19a'
            self._parsed_conn = self._parse_common_connection_str(conn_str, current, self._URI_SCHEMA_NAME, self._MANDATORY_KEY, self._ALT_URI_SCHEMA_NAMES, self._ALL_KEYS, self._VALID_KEYS_COMBINATIONS)
            self.client = Kusto_Client(self._parsed_conn)

    def get_client(self):
        if self.client is None:
            cluster_connection = self.conn_class.get_connection_by_name("@" + self.cluster_name)
            if cluster_connection is None:
                raise KqlEngineError("connection to cluster not set.")
            return cluster_connection.get_client()
        else:
            return self.client

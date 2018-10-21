#-------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
#--------------------------------------------------------------------------

from Kqlmagic.kql_engine import KqlEngine, KqlEngineError
from Kqlmagic.kusto_client import Kusto_Client
from Kqlmagic.constants import ConnStrKeys


class KustoEngine(KqlEngine):

    # Constants
    # ---------

    _URI_SCHEMA_NAME = "kusto"
    _ALT_URI_SCHEMA_NAMES = [_URI_SCHEMA_NAME, "adx", "ade", "azuredataexplorer", "azure_data_explorer"]
    _MANDATORY_KEY = ConnStrKeys.DATABASE
    _VALID_KEYS_COMBINATIONS = [
            [ConnStrKeys.TENANT, ConnStrKeys.CODE, ConnStrKeys.CLUSTER, ConnStrKeys.DATABASE, ConnStrKeys.ALIAS],
            [ConnStrKeys.TENANT, ConnStrKeys.USERNAME, ConnStrKeys.PASSWORD, ConnStrKeys.CLUSTER, ConnStrKeys.DATABASE, ConnStrKeys.ALIAS],
            [ConnStrKeys.TENANT, ConnStrKeys.CLIENTID, ConnStrKeys.CLIENTSECRET, ConnStrKeys.CLUSTER, ConnStrKeys.DATABASE, ConnStrKeys.ALIAS],
            [ConnStrKeys.TENANT, ConnStrKeys.CLIENTID, ConnStrKeys.CERTIFICATE, ConnStrKeys.CERTIFICATE_THUMBPRINT, ConnStrKeys.CLUSTER, ConnStrKeys.DATABASE, ConnStrKeys.ALIAS],
    ]

    # Class methods
    # -------------

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

    # Instance methods
    # ----------------

    def __init__(self, conn_str, current=None, conn_class=None):
        super().__init__()
        if isinstance(conn_str, dict):
            self.conn_class = conn_class
            self.database_name = conn_str.get(ConnStrKeys.DATABASE)
            self.cluster_name = conn_str.get(ConnStrKeys.CLUSTER)
            self.alias = conn_str.get(ConnStrKeys.ALIAS)
            self.bind_url = "{0}://{1}('{2}').{3}('{4}')".format(
                self._URI_SCHEMA_NAME, 
                ConnStrKeys.CLUSTER,
                self.cluster_name,
                ConnStrKeys.DATABASE,
                self.database_name)
        else:
            self._parsed_conn = self._parse_common_connection_str(conn_str, current, self._URI_SCHEMA_NAME, self._MANDATORY_KEY, self._ALT_URI_SCHEMA_NAMES, self._VALID_KEYS_COMBINATIONS)
            self.client = Kusto_Client(self._parsed_conn)

    def get_client(self):
        if self.client is None:
            cluster_connection = self.conn_class.get_connection_by_name("@" + self.cluster_name)
            if cluster_connection is None:
                raise KqlEngineError("connection to cluster not set.")
            return cluster_connection.get_client()
        else:
            return self.client

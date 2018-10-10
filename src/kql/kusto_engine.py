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
            schema = "kusto"
            mandatory_key = "database"
            not_in_url_key = "database"
            self._parse_common_connection_str(conn_str, current, schema, mandatory_key, not_in_url_key)
            self.client = Kusto_Client(self._parsed_conn)

    def get_client(self):
        if self.client is None:
            cluster_connection = self.conn_class.get_connection_by_name("@" + self.cluster_name)
            if cluster_connection is None:
                raise KqlEngineError("connection to cluster not set.")
            return cluster_connection.get_client()
        else:
            return self.client

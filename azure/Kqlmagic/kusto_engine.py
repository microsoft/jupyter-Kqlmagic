# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

from Kqlmagic.kql_engine import KqlEngine, KqlEngineError
from Kqlmagic.kusto_client import Kusto_Client
from Kqlmagic.constants import ConnStrKeys


class KustoEngine(KqlEngine):

    # Constants
    # ---------

    _URI_SCHEMA_NAME = "azuredataexplorer" # no spaces, underscores, and hyphe-minus, because they are ignored in parser
    _ALT_URI_SCHEMA1_NAME = "adx" # no spaces, underscores, and hyphe-minus, because they are ignored in parser
    _ALT_URI_SCHEMA2_NAME = "ade" # no spaces, underscores, and hyphe-minus, because they are ignored in parser
    _ALT_URI_SCHEMA3_NAME = "kusto" # no spaces, underscores, and hyphe-minus, because they are ignored in parser

    _ALT_URI_SCHEMA_NAMES = [_URI_SCHEMA_NAME, _ALT_URI_SCHEMA1_NAME, _ALT_URI_SCHEMA2_NAME, _ALT_URI_SCHEMA3_NAME]
    _MANDATORY_KEY = ConnStrKeys.DATABASE
    _VALID_KEYS_COMBINATIONS = [
        [ConnStrKeys.TENANT,ConnStrKeys.AAD_URL,  ConnStrKeys.ANONYMOUS, ConnStrKeys.CLUSTER, ConnStrKeys.DATABASE, ConnStrKeys.ALIAS],

        [ConnStrKeys.TENANT,ConnStrKeys.AAD_URL,  ConnStrKeys.CODE, ConnStrKeys.CLUSTER, ConnStrKeys.DATABASE, ConnStrKeys.ALIAS],
        [ConnStrKeys.TENANT,ConnStrKeys.AAD_URL,  ConnStrKeys.CODE,ConnStrKeys.CLIENTID, ConnStrKeys.CLUSTER, ConnStrKeys.DATABASE, ConnStrKeys.ALIAS],


        [ConnStrKeys.TENANT,ConnStrKeys.AAD_URL,  ConnStrKeys.USERNAME, ConnStrKeys.PASSWORD, ConnStrKeys.CLUSTER, ConnStrKeys.DATABASE, ConnStrKeys.ALIAS],
        [ConnStrKeys.TENANT,ConnStrKeys.AAD_URL, ConnStrKeys.CLIENTID, ConnStrKeys.CLIENTSECRET, ConnStrKeys.CLUSTER, ConnStrKeys.DATABASE, ConnStrKeys.ALIAS],
        [
            ConnStrKeys.TENANT,
            ConnStrKeys.AAD_URL, 
            ConnStrKeys.CLIENTID,
            ConnStrKeys.CERTIFICATE,
            ConnStrKeys.CERTIFICATE_THUMBPRINT,
            ConnStrKeys.CLUSTER,
            ConnStrKeys.DATABASE,
            ConnStrKeys.ALIAS
        ],
    ]

    # Class methods
    # -------------

    # Instance methods
    # ----------------

    def __init__(self, conn_str, user_ns: dict, current=None, conn_class=None,  **options):
        super().__init__()
        if isinstance(conn_str, dict):
            self.conn_class = conn_class
            self.database_name = conn_str.get(ConnStrKeys.DATABASE)
            self.database_friendly_name = self.createDatabaseFriendlyName(self.database_name)
            self.cluster_name = conn_str.get(ConnStrKeys.CLUSTER)
            self.alias = conn_str.get(ConnStrKeys.ALIAS) or self.database_friendly_name
            self.cluster_friendly_name = conn_str.get("cluster_friendly_name")
            self.bind_url = "{0}://{1}('{2}').{3}('{4}')".format(
                self._URI_SCHEMA_NAME, ConnStrKeys.CLUSTER, self.cluster_name, ConnStrKeys.DATABASE, self.database_name
            )
        else:
            self._parsed_conn = self._parse_common_connection_str(
                conn_str, current, self._URI_SCHEMA_NAME, self._MANDATORY_KEY, self._VALID_KEYS_COMBINATIONS, user_ns
            )

            self.client = Kusto_Client(self._parsed_conn, **options)

    def get_client(self):
        if self.client is None:
            cluster_connection = self.conn_class.get_connection_by_name("@" + self.cluster_friendly_name)
            if cluster_connection is None:
                raise KqlEngineError("connection to cluster not set.")
            return cluster_connection.get_client()
        else:
            return self.client

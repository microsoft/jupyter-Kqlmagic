# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

import urllib.parse


from .kql_engine import KqlEngine, KqlEngineError
from .kusto_client import Kusto_Client
from .constants import ConnStrKeys, Schema


class KustoEngine(KqlEngine):

    # Constants
    # ---------

    _URI_SCHEMA_NAME = Schema.AZURE_DATA_EXPLORER # no spaces, underscores, and hyphe-minus, because they are ignored in parser
    _ALT_URI_SCHEMA1_NAME = "adx" # no spaces, underscores, and hyphe-minus, because they are ignored in parser
    _ALT_URI_SCHEMA2_NAME = "ade" # no spaces, underscores, and hyphe-minus, because they are ignored in parser
    _ALT_URI_SCHEMA3_NAME = "kusto" # no spaces, underscores, and hyphe-minus, because they are ignored in parser

    _ALT_URI_SCHEMA_NAMES = [_URI_SCHEMA_NAME, _ALT_URI_SCHEMA1_NAME, _ALT_URI_SCHEMA2_NAME, _ALT_URI_SCHEMA3_NAME]
    _MANDATORY_KEY = ConnStrKeys.DATABASE
    _VALID_KEYS_COMBINATIONS = [
        [ConnStrKeys.DATABASE, ConnStrKeys.ALIAS, ConnStrKeys.CLUSTER, ConnStrKeys.TENANT, ConnStrKeys.AAD_URL, ConnStrKeys.CLIENTID, ConnStrKeys.CLIENTSECRET],
        [ConnStrKeys.DATABASE, ConnStrKeys.ALIAS, ConnStrKeys.CLUSTER, ConnStrKeys.TENANT, ConnStrKeys.AAD_URL, ConnStrKeys.CLIENTID, ConnStrKeys.CERTIFICATE, ConnStrKeys.CERTIFICATE_THUMBPRINT],
        [ConnStrKeys.DATABASE, ConnStrKeys.ALIAS, ConnStrKeys.CLUSTER, ConnStrKeys.TENANT, ConnStrKeys.AAD_URL, ConnStrKeys.CLIENTID, ConnStrKeys.CODE],
        [ConnStrKeys.DATABASE, ConnStrKeys.ALIAS, ConnStrKeys.CLUSTER, ConnStrKeys.TENANT, ConnStrKeys.AAD_URL,                       ConnStrKeys.CODE],
        [ConnStrKeys.DATABASE, ConnStrKeys.ALIAS, ConnStrKeys.CLUSTER, ConnStrKeys.SUBSCRIPTION,                                      ConnStrKeys.AZCLI],
        [ConnStrKeys.DATABASE, ConnStrKeys.ALIAS, ConnStrKeys.CLUSTER, ConnStrKeys.TENANT,                                            ConnStrKeys.AZCLI],
        [ConnStrKeys.DATABASE, ConnStrKeys.ALIAS, ConnStrKeys.CLUSTER,                                                                ConnStrKeys.AZCLI],
        [ConnStrKeys.DATABASE, ConnStrKeys.ALIAS, ConnStrKeys.CLUSTER, ConnStrKeys.TENANT, ConnStrKeys.AAD_URL, ConnStrKeys.CLIENTID, ConnStrKeys.USERNAME, ConnStrKeys.PASSWORD],
        [ConnStrKeys.DATABASE, ConnStrKeys.ALIAS, ConnStrKeys.CLUSTER, ConnStrKeys.TENANT, ConnStrKeys.AAD_URL,                       ConnStrKeys.USERNAME, ConnStrKeys.PASSWORD],
        [ConnStrKeys.DATABASE, ConnStrKeys.ALIAS, ConnStrKeys.CLUSTER,                                                                ConnStrKeys.ANONYMOUS],
    ]

    _VALID_KEYS_COMBINATIONS_NEW = [
        {
            "must": [ConnStrKeys.DATABASE, ConnStrKeys.CLUSTER, ConnStrKeys.CLIENTID, ConnStrKeys.CLIENTSECRET],
            "extra": [],
            "optional": [ConnStrKeys.ALIAS, ConnStrKeys.TENANT, ConnStrKeys.AAD_URL]
        },
        {
            "must": [ConnStrKeys.DATABASE, ConnStrKeys.CLUSTER, ConnStrKeys.CLIENTID, ConnStrKeys.CERTIFICATE, ConnStrKeys.CERTIFICATE_THUMBPRINT],
            "extra": [],
            "optional": [ConnStrKeys.ALIAS, ConnStrKeys.TENANT, ConnStrKeys.AAD_URL]
        },
        {
            "must": [ConnStrKeys.DATABASE, ConnStrKeys.CLUSTER, ConnStrKeys.CODE],
            "extra": [ConnStrKeys.CLIENTID, ConnStrKeys.USERNAME],
            "optional": [ConnStrKeys.ALIAS, ConnStrKeys.TENANT, ConnStrKeys.AAD_URL]
        },
        {
            "must": [ConnStrKeys.DATABASE, ConnStrKeys.CLUSTER, ConnStrKeys.USERNAME, ConnStrKeys.PASSWORD],
            "extra": [ConnStrKeys.CLIENTID],
            "optional": [ConnStrKeys.ALIAS, ConnStrKeys.TENANT, ConnStrKeys.AAD_URL, ConnStrKeys.CLIENTID]
        },
        {
            "must": [ConnStrKeys.DATABASE, ConnStrKeys.CLUSTER, ConnStrKeys.ANONYMOUS],
            "extra": [],
            "optional": [ConnStrKeys.ALIAS]
        }
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
            self.bind_url = f"{self._URI_SCHEMA_NAME}://{ConnStrKeys.CLUSTER}('{self.cluster_name}').{ConnStrKeys.DATABASE}('{self.database_name}')"
        else:
            self._parsed_conn = self._parse_common_connection_str(
                conn_str, current, self._URI_SCHEMA_NAME, self._MANDATORY_KEY, self._VALID_KEYS_COMBINATIONS, user_ns
            )

            self.client = Kusto_Client(self._parsed_conn, **options)


    def get_client(self):
        if self.client is None:
            cluster_connection = self.conn_class.get_connection_by_name(f"@{self.cluster_friendly_name}")
            if cluster_connection is None:
                raise KqlEngineError("connection to cluster not set.")
            return cluster_connection.get_client()
        else:
            return self.client


    def get_deep_link(self, query: str, options) -> str:
        client = self.get_client()
        http_query = []
        web_or_app = 0 if options.get("query_link_destination") == "Kusto.Explorer" else 1 # default "Kusto.WebExplorer"
        http_query.append(f"web={urllib.parse.quote(str(web_or_app))}")
        # http_query.append(f"uri={urllib.parse.quote(client.data_source)}") # not clear what it should be 
        http_query.append(f"name={urllib.parse.quote(self.cluster_friendly_name)}")
        if options.get("saw"):
            http_query.append(f"saw={urllib.parse.quote(str(options.get('saw')))}")

        http_query.append(f"query={urllib.parse.quote(query)}")

            
        url = f"{client.deep_link_data_source}/{self.database_name}?{'&'.join(http_query)}"
        # print (f'deep link: {url}')
        return url


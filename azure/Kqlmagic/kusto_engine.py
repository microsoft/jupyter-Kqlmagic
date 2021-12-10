# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

from typing import Any, Union, Tuple, Dict, List
import urllib.parse


from .kql_engine import KqlEngine
from .exceptions import KqlEngineError
from .kusto_client import KustoClient
from .constants import ConnStrKeys, Schema


class KustoEngine(KqlEngine):

    # Constants
    # ---------

    _URI_SCHEMA_NAME = Schema.AZURE_DATA_EXPLORER  # no spaces, underscores, and hyphe-minus, because they are ignored in parser
    _ALT_URI_SCHEMA1_NAME = "adx"  # no spaces, underscores, and hyphe-minus, because they are ignored in parser
    _ALT_URI_SCHEMA2_NAME = "ade"  # no spaces, underscores, and hyphe-minus, because they are ignored in parser
    _ALT_URI_SCHEMA3_NAME = "kusto"  # no spaces, underscores, and hyphe-minus, because they are ignored in parser

    _DEFAULT_CLUSTER_NAME = None

    _ALT_URI_SCHEMA_NAMES = [_URI_SCHEMA_NAME, _ALT_URI_SCHEMA1_NAME, _ALT_URI_SCHEMA2_NAME, _ALT_URI_SCHEMA3_NAME]
    _MANDATORY_KEY = ConnStrKeys.DATABASE
    _VALID_KEYS_COMBINATIONS = [
        [ConnStrKeys.DATABASE, ConnStrKeys.ALIAS, ConnStrKeys.CLUSTER, ConnStrKeys.TENANT, ConnStrKeys.AAD_URL, ConnStrKeys.CLIENTID, ConnStrKeys.CLIENTSECRET],
        [ConnStrKeys.DATABASE, ConnStrKeys.ALIAS, ConnStrKeys.CLUSTER, ConnStrKeys.TENANT, ConnStrKeys.AAD_URL, ConnStrKeys.CLIENTID, ConnStrKeys.CERTIFICATE, ConnStrKeys.CERTIFICATE_THUMBPRINT],
        [ConnStrKeys.DATABASE, ConnStrKeys.ALIAS, ConnStrKeys.CLUSTER, ConnStrKeys.TENANT, ConnStrKeys.AAD_URL, ConnStrKeys.CLIENTID, ConnStrKeys.CODE],
        [ConnStrKeys.DATABASE, ConnStrKeys.ALIAS, ConnStrKeys.CLUSTER, ConnStrKeys.TENANT, ConnStrKeys.AAD_URL,                       ConnStrKeys.CODE],
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

    def __init__(self, conn_str_or_dict:Union[str,Dict[str,str]], user_ns:Dict[str,Any], current:KqlEngine=None, conn_class:Any=None, **options)->None:
        super(KustoEngine, self).__init__()
        self.conn_class = conn_class
        if isinstance(conn_str_or_dict, dict):
            conn_dict:Dict[str,str] = conn_str_or_dict
            self.database_name = conn_dict.get(ConnStrKeys.DATABASE)
            self.database_friendly_name = self.createDatabaseFriendlyName(self.database_name)
            self.cluster_name = conn_dict.get(ConnStrKeys.CLUSTER)
            self.alias = conn_dict.get(ConnStrKeys.ALIAS) or self.database_friendly_name
            self.cluster_friendly_name = conn_dict.get(ConnStrKeys.CLUSTER_FRIENDLY_NAME)
            self.bind_url = f"{self._URI_SCHEMA_NAME}://{ConnStrKeys.CLUSTER}('{self.cluster_name}').{ConnStrKeys.DATABASE}('{self.database_name}')"
        else:
            conn_str:str = conn_str_or_dict
            self._parsed_conn = self._parse_common_connection_str(
                conn_str, current, self._URI_SCHEMA_NAME, self._MANDATORY_KEY, self._VALID_KEYS_COMBINATIONS, user_ns
            )

            cluster_name = self._parsed_conn.get(ConnStrKeys.CLUSTER, self._DEFAULT_CLUSTER_NAME)
            self.client = KustoClient(cluster_name, self._parsed_conn, **options)


    # collect datails, in case bug report will be generated
    def get_details(self)->Dict[str,Any]:
        if self.client is None:
            cluster_engine:KqlEngine = self._get_cluster_engine()
            details = cluster_engine.get_details()
            details["database_name"] = self.database_name
        else:
            details = super(KustoEngine, self).get_details()
            details["parsed_conn"] = self.obfuscate_parsed_conn()
            if details["auth"] is None:
                client = self.get_client()
                if client and client._aad_helper:
                    details["auth"] = client._aad_helper.get_details()
        return details


    def get_client(self)->KustoClient:
        if self.client is None:
            cluster_engine:KqlEngine = self._get_cluster_engine()
            if cluster_engine is None:
                raise KqlEngineError("connection to cluster not set.")
            return cluster_engine.get_client()
        else:
            return self.client


    def get_deep_link(self, query:str, options:Dict[str,Any]=None)->str:
        options = options or {}
        client = self.get_client()
        http_query = []
        web_or_app = 0 if options.get("query_link_destination") == "Kusto.Explorer" else 1  # default "Kusto.WebExplorer"
        http_query.append(f"web={urllib.parse.quote(str(web_or_app))}")
        # http_query.append(f"uri={urllib.parse.quote(client.data_source)}")  # not clear what it should be 
        http_query.append(f"name={urllib.parse.quote(self.cluster_friendly_name)}")
        if options.get("saw"):
            http_query.append(f"saw={urllib.parse.quote(str(options.get('saw')))}")

        http_query.append(f"query={urllib.parse.quote(query)}")

        url = f"{client.deep_link_data_source}/{self.database_name}?{'&'.join(http_query)}"
        return url


    def _get_cluster_engine(self)->KqlEngine:
        # conn_class is Connection class
        return self.conn_class.get_engine_by_name(f"@{self.cluster_friendly_name}")


    def _get_databases_by_pretty_name(self, **options)->Tuple[List[str],Dict[str,str]]:
        query = ".show databases"
        kql_response = self.execute(query, database="NetDefaultDB", **options)
        table = kql_response.tables[0]
        databases_by_pretty_name = {row['PrettyName']: row['DatabaseName'] for row in table.fetchall() if row['PrettyName']}
        database_name_list = [row['DatabaseName'] for row in table.fetchall()]
        return database_name_list, databases_by_pretty_name

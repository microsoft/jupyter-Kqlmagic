# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

from .kql_engine import KqlEngine, KqlEngineError
from .draft_client import DraftClient
from .constants import ConnStrKeys, Schema


class LoganalyticsEngine(KqlEngine):

    # Constants
    # ---------

    _URI_SCHEMA_NAME = Schema.LOG_ANALYTICS # no spaces, underscores, and hyphe-minus, because they are ignored in parser
    _DOMAIN = "workspaces"
    _DATA_SOURCE = "https://api.loganalytics.io"
    

    _ALT_URI_SCHEMA_NAMES = [_URI_SCHEMA_NAME]
    _MANDATORY_KEY = ConnStrKeys.WORKSPACE
    _VALID_KEYS_COMBINATIONS = [
        [ConnStrKeys.WORKSPACE, ConnStrKeys.ALIAS, ConnStrKeys.DATA_SOURCE_URL, ConnStrKeys.TENANT, ConnStrKeys.AAD_URL, ConnStrKeys.CLIENTID, ConnStrKeys.CLIENTSECRET], 
        [ConnStrKeys.WORKSPACE, ConnStrKeys.ALIAS, ConnStrKeys.DATA_SOURCE_URL, ConnStrKeys.TENANT, ConnStrKeys.AAD_URL, ConnStrKeys.CLIENTID, ConnStrKeys.CODE],
        [ConnStrKeys.WORKSPACE, ConnStrKeys.ALIAS, ConnStrKeys.DATA_SOURCE_URL, ConnStrKeys.TENANT, ConnStrKeys.AAD_URL,                       ConnStrKeys.CODE],
        [ConnStrKeys.WORKSPACE, ConnStrKeys.ALIAS, ConnStrKeys.DATA_SOURCE_URL, ConnStrKeys.SUBSCRIPTION,                                      ConnStrKeys.AZCLI],
        [ConnStrKeys.WORKSPACE, ConnStrKeys.ALIAS, ConnStrKeys.DATA_SOURCE_URL, ConnStrKeys.TENANT,                                            ConnStrKeys.AZCLI],
        [ConnStrKeys.WORKSPACE, ConnStrKeys.ALIAS, ConnStrKeys.DATA_SOURCE_URL,                                                                ConnStrKeys.AZCLI],
        [ConnStrKeys.WORKSPACE, ConnStrKeys.ALIAS, ConnStrKeys.DATA_SOURCE_URL, ConnStrKeys.TENANT, ConnStrKeys.AAD_URL, ConnStrKeys.CLIENTID, ConnStrKeys.USERNAME, ConnStrKeys.PASSWORD],
        [ConnStrKeys.WORKSPACE, ConnStrKeys.ALIAS, ConnStrKeys.DATA_SOURCE_URL, ConnStrKeys.TENANT, ConnStrKeys.AAD_URL,                       ConnStrKeys.USERNAME, ConnStrKeys.PASSWORD],
        [ConnStrKeys.WORKSPACE, ConnStrKeys.ALIAS, ConnStrKeys.DATA_SOURCE_URL,                                                                ConnStrKeys.ANONYMOUS],
        [ConnStrKeys.WORKSPACE, ConnStrKeys.ALIAS, ConnStrKeys.DATA_SOURCE_URL,                                                                ConnStrKeys.APPKEY],
    ]

    _VALID_KEYS_COMBINATIONS_NEW = [
        {
            "must": [ConnStrKeys.WORKSPACE, ConnStrKeys.CLIENTID, ConnStrKeys.CLIENTSECRET],
            "extra": [],
            "optional": [ConnStrKeys.ALIAS, ConnStrKeys.DATA_SOURCE_URL, ConnStrKeys.TENANT, ConnStrKeys.AAD_URL]
        },
        {
            "must": [ConnStrKeys.WORKSPACE, ConnStrKeys.CODE],
            "extra": [ConnStrKeys.USERNAME, ConnStrKeys.CLIENTID],
            "optional": [ConnStrKeys.ALIAS, ConnStrKeys.DATA_SOURCE_URL, ConnStrKeys.TENANT, ConnStrKeys.AAD_URL]
        },
        {
            "must": [ConnStrKeys.WORKSPACE, ConnStrKeys.USERNAME, ConnStrKeys.PASSWORD],
            "extra": [ConnStrKeys.CLIENTID],
            "optional": [ConnStrKeys.ALIAS, ConnStrKeys.DATA_SOURCE_URL, ConnStrKeys.TENANT, ConnStrKeys.AAD_URL]
        },
        {
            "must": [ConnStrKeys.WORKSPACE, ConnStrKeys.ANONYMOUS],
            "extra": [],
            "optional": [ConnStrKeys.ALIAS, ConnStrKeys.DATA_SOURCE_URL]
        },
        {
            "must": [ConnStrKeys.WORKSPACE, ConnStrKeys.APPKEY],
            "extra": [],
            "optional": [ConnStrKeys.ALIAS, ConnStrKeys.DATA_SOURCE_URL]
        },
    ]


    # Class methods
    # -------------

    # Instance methods
    # ----------------

    def __init__(self, conn_str, user_ns: dict, current=None, **options):
        super().__init__()
        self._parsed_conn = self._parse_common_connection_str(
            conn_str, current, self._URI_SCHEMA_NAME, self._MANDATORY_KEY, self._VALID_KEYS_COMBINATIONS, user_ns
        )
        self.client = DraftClient(self._parsed_conn, self._DOMAIN, self._DATA_SOURCE, self._URI_SCHEMA_NAME, **options)

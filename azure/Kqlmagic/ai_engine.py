# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

import requests


from .kql_engine import KqlEngine, KqlEngineError
from .draft_client import DraftClient
from .constants import ConnStrKeys, Schema
from .log import logger


class AppinsightsEngine(KqlEngine):

    # Constants
    # ---------
    _URI_SCHEMA_NAME = Schema.APPLICATION_INSIGHTS # no spaces, underscores, and hyphe-minus, because they are ignored in parser
    _ALT_URI_SCHEMA_NAME = "appinsights" # no spaces, underscores, and hyphe-minus, because they are ignored in parser
    _DOMAIN = "apps"
    
    _DATA_SOURCE = "https://api.applicationinsights.io"
 
    _ALT_URI_SCHEMA_NAMES = [_URI_SCHEMA_NAME, _ALT_URI_SCHEMA_NAME]
    _MANDATORY_KEY = ConnStrKeys.APPID
    _VALID_KEYS_COMBINATIONS = [
        [ConnStrKeys.APPID, ConnStrKeys.ALIAS, ConnStrKeys.DATA_SOURCE_URL, ConnStrKeys.TENANT, ConnStrKeys.AAD_URL, ConnStrKeys.CLIENTID, ConnStrKeys.CLIENTSECRET], 
        [ConnStrKeys.APPID, ConnStrKeys.ALIAS, ConnStrKeys.DATA_SOURCE_URL, ConnStrKeys.TENANT, ConnStrKeys.AAD_URL, ConnStrKeys.CLIENTID, ConnStrKeys.CODE],
        [ConnStrKeys.APPID, ConnStrKeys.ALIAS, ConnStrKeys.DATA_SOURCE_URL, ConnStrKeys.TENANT, ConnStrKeys.AAD_URL,                       ConnStrKeys.CODE],
        [ConnStrKeys.APPID, ConnStrKeys.ALIAS, ConnStrKeys.DATA_SOURCE_URL, ConnStrKeys.TENANT, ConnStrKeys.AAD_URL, ConnStrKeys.CLIENTID, ConnStrKeys.USERNAME, ConnStrKeys.PASSWORD],
        [ConnStrKeys.APPID, ConnStrKeys.ALIAS, ConnStrKeys.DATA_SOURCE_URL, ConnStrKeys.TENANT, ConnStrKeys.AAD_URL,                       ConnStrKeys.USERNAME, ConnStrKeys.PASSWORD],
        [ConnStrKeys.APPID, ConnStrKeys.ALIAS, ConnStrKeys.DATA_SOURCE_URL,                                                                ConnStrKeys.ANONYMOUS],
        [ConnStrKeys.APPID, ConnStrKeys.ALIAS, ConnStrKeys.DATA_SOURCE_URL,                                                                ConnStrKeys.APPKEY],
    ]

    _VALID_KEYS_COMBINATIONS_NEW = [
        {
            "required": [ConnStrKeys.APPID, ConnStrKeys.CLIENTID, ConnStrKeys.CLIENTSECRET],
            "extra": [],
            "optional": [ConnStrKeys.ALIAS, ConnStrKeys.DATA_SOURCE_URL, ConnStrKeys.TENANT, ConnStrKeys.AAD_URL]
        },
        {
            "required": [ConnStrKeys.APPID, ConnStrKeys.CODE],
            "extra": [ConnStrKeys.CLIENTID, ConnStrKeys.USERNAME],
            "optional": [ConnStrKeys.ALIAS, ConnStrKeys.DATA_SOURCE_URL, ConnStrKeys.TENANT, ConnStrKeys.AAD_URL]
        },
        {
            "required": [ConnStrKeys.APPID, ConnStrKeys.USERNAME, ConnStrKeys.PASSWORD],
            "extra": [ConnStrKeys.CLIENTID],
            "optional": [ConnStrKeys.ALIAS, ConnStrKeys.DATA_SOURCE_URL, ConnStrKeys.TENANT, ConnStrKeys.AAD_URL]
        },
        {
            "required": [ConnStrKeys.APPID, ConnStrKeys.ANONYMOUS],
            "extra": [],
            "optional": [ConnStrKeys.ALIAS, ConnStrKeys.DATA_SOURCE_URL]
        },
        {
            "required": [ConnStrKeys.APPID, ConnStrKeys.APPKEY],
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

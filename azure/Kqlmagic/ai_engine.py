# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

from Kqlmagic.kql_engine import KqlEngine, KqlEngineError
from Kqlmagic.draft_client import DraftClient
from Kqlmagic.constants import ConnStrKeys
from Kqlmagic.log import logger

import requests


class AppinsightsEngine(KqlEngine):

    # Constants
    # ---------
    _URI_SCHEMA_NAME = "applicationinsights" # no spaces, underscores, and hyphe-minus, because they are ignored in parser
    _ALT_URI_SCHEMA_NAME = "appinsights" # no spaces, underscores, and hyphe-minus, because they are ignored in parser
    _DOMAIN = "apps"
    
    _DATA_SOURCE = "https://api.applicationinsights.io"
 
    _ALT_URI_SCHEMA_NAMES = [_URI_SCHEMA_NAME, _ALT_URI_SCHEMA_NAME]
    _MANDATORY_KEY = ConnStrKeys.APPID
    _VALID_KEYS_COMBINATIONS = [
        [ConnStrKeys.TENANT, ConnStrKeys.AAD_URL, ConnStrKeys.DATA_SOURCE_URL, ConnStrKeys.ANONYMOUS, ConnStrKeys.APPID, ConnStrKeys.ALIAS],
        [ConnStrKeys.TENANT,ConnStrKeys.AAD_URL, ConnStrKeys.DATA_SOURCE_URL, ConnStrKeys.CODE,ConnStrKeys.CLIENTID, ConnStrKeys.APPID, ConnStrKeys.ALIAS],
        [ConnStrKeys.TENANT,ConnStrKeys.AAD_URL, ConnStrKeys.DATA_SOURCE_URL, ConnStrKeys.CODE,ConnStrKeys.APPID, ConnStrKeys.ALIAS],

        [ConnStrKeys.TENANT,ConnStrKeys.AAD_URL, ConnStrKeys.DATA_SOURCE_URL, ConnStrKeys.CLIENTID, ConnStrKeys.CLIENTSECRET, ConnStrKeys.APPID, ConnStrKeys.ALIAS],
        
        [ConnStrKeys.APPID, ConnStrKeys.APPKEY, ConnStrKeys.ALIAS, ConnStrKeys.DATA_SOURCE_URL],
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
        logger().debug("ai_engine.py :: __init__ :  self._parsed_conn: {0}".format(self._parsed_conn))
        self.client = DraftClient(self._parsed_conn, self._DOMAIN, self._DATA_SOURCE, **options)
        logger().debug("ai_engine.py :: __init__ :  self.client: {0}".format(self.client))

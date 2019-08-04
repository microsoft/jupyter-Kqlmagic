# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

from Kqlmagic.kql_engine import KqlEngine, KqlEngineError
from Kqlmagic.draft_client import DraftClient
from Kqlmagic.constants import ConnStrKeys

import requests


class AppinsightsEngine(KqlEngine):

    # Constants
    # ---------
    _URI_SCHEMA_NAME = "applicationinsights" # no spaces, underscores, and hyphe-minus, because they are ignored in parser
    _ALT_URI_SCHEMA_NAME = "appinsights" # no spaces, underscores, and hyphe-minus, because they are ignored in parser
    _DOMAIN = "apps"
    _DATA_SOURCE = "https://api.applicationinsights.{0}"
 
    _ALT_URI_SCHEMA_NAMES = [_URI_SCHEMA_NAME, _ALT_URI_SCHEMA_NAME]
    _MANDATORY_KEY = ConnStrKeys.APPID
    _VALID_KEYS_COMBINATIONS = [
        [ConnStrKeys.TENANT, ConnStrKeys.ANONYMOUS, ConnStrKeys.APPID, ConnStrKeys.ALIAS],
        [ConnStrKeys.TENANT, ConnStrKeys.CODE,ConnStrKeys.CLIENTID, ConnStrKeys.APPID, ConnStrKeys.ALIAS],
        [ConnStrKeys.TENANT, ConnStrKeys.CODE,ConnStrKeys.WORKSPACE, ConnStrKeys.ALIAS],

        [ConnStrKeys.TENANT, ConnStrKeys.CLIENTID, ConnStrKeys.CLIENTSECRET, ConnStrKeys.APPID, ConnStrKeys.ALIAS],
        [ConnStrKeys.APPID, ConnStrKeys.APPKEY, ConnStrKeys.ALIAS],
    ]

    # Class methods
    # -------------

    # Instance methods
    # ----------------

    def __init__(self, conn_str, user_ns: dict, current=None, cloud=None):
        super().__init__()
        self._parsed_conn = self._parse_common_connection_str(
            conn_str, current, self._URI_SCHEMA_NAME, self._MANDATORY_KEY, self._VALID_KEYS_COMBINATIONS, user_ns
        )
        self.client = DraftClient(self._parsed_conn, self._DOMAIN, self._DATA_SOURCE, cloud)

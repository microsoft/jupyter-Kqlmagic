#-------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
#--------------------------------------------------------------------------

import os.path
from Kqlmagic.kql_engine import KqlEngine, KqlEngineError
from Kqlmagic.draft_client import DraftClient
import requests


class AppinsightsEngine(KqlEngine):
    _URI_SCHEMA_NAME = "appinsights"
    _ALT_URI_SCHEMA_NAMES = [_URI_SCHEMA_NAME, "app_insights", "applicationinsights", "application_insights"]
    _DOMAIN = "apps"
    _MANDATORY_KEY = "appid"
    _VALID_KEYS_COMBINATIONS = [
            ["tenant", "code", "appid", "alias"],
            ["tenant", "clientid", "clientsecret", "appid", "alias"],
            ["appid", "appkey", "alias"],
    ]
    _ALL_KEYS = set()
    for c in _VALID_KEYS_COMBINATIONS:
        _ALL_KEYS.update(set(c))

    _API_URI = "https://api.applicationinsights.io"

    @classmethod
    def tell_format(cls):
        return """
               appinsights://appid('appid').appkey('appkey')

               ## Note: if appkey is missing, user will be prompted to enter appkey"""

    # Object constructor
    def __init__(self, conn_str, current=None):
        super().__init__()
        self._parsed_conn = self._parse_common_connection_str(conn_str, current, self._URI_SCHEMA_NAME, self._MANDATORY_KEY, self._ALT_URI_SCHEMA_NAMES, self._ALL_KEYS, self._VALID_KEYS_COMBINATIONS)
        self.client = DraftClient(self._parsed_conn, self._DOMAIN, self._API_URI)

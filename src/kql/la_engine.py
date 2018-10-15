#-------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
#--------------------------------------------------------------------------

from kql.kql_engine import KqlEngine, KqlEngineError
from kql.draft_client import DraftClient

class LoganalyticsEngine(KqlEngine):
    _URI_SCHEMA_NAME = "loganalytics"
    _ALT_URI_SCHEMA_NAMES = [_URI_SCHEMA_NAME, "log_analytics"]
    _DOMAIN = "workspaces"
    _MANDATORY_KEY = "workspace"
    _VALID_KEYS_COMBINATIONS = [
            ["tenant", "code", "workspace", "alias"],
            ["tenant", "clientid", "clientsecret", "workspace", "alias"],
            ["workspace", "appkey", "alias"], # only for demo, if workspace = "DEMO_WORKSPACE"
    ]
    _ALL_KEYS = set()
    for c in _VALID_KEYS_COMBINATIONS:
        _ALL_KEYS.update(set(c))

    _API_URI = "https://api.loganalytics.io"

    @classmethod
    def tell_format(cls):
        return """
               loganalytics://workspace('workspaceid').appkey('appkey')

               ## Note: if appkey is missing, user will be prompted to enter appkey"""

    # Object constructor
    def __init__(self, conn_str, current=None):
        super().__init__()
        self._parsed_conn = self._parse_common_connection_str(conn_str, current, self._URI_SCHEMA_NAME, self._MANDATORY_KEY, self._ALT_URI_SCHEMA_NAMES, self._ALL_KEYS, self._VALID_KEYS_COMBINATIONS)
        self.client = DraftClient(self._parsed_conn, self._DOMAIN, self._API_URI)

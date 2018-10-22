# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

from Kqlmagic.kql_engine import KqlEngine, KqlEngineError
from Kqlmagic.draft_client import DraftClient
from Kqlmagic.constants import ConnStrKeys


class LoganalyticsEngine(KqlEngine):

    # Constants
    # ---------

    _URI_SCHEMA_NAME = "loganalytics"
    _ALT_URI_SCHEMA_NAMES = [_URI_SCHEMA_NAME, "log_analytics"]
    _DOMAIN = "workspaces"
    _MANDATORY_KEY = ConnStrKeys.WORKSPACE
    _VALID_KEYS_COMBINATIONS = [
        [ConnStrKeys.TENANT, ConnStrKeys.CODE, ConnStrKeys.WORKSPACE, ConnStrKeys.ALIAS],
        [ConnStrKeys.TENANT, ConnStrKeys.CLIENTID, ConnStrKeys.CLIENTSECRET, ConnStrKeys.WORKSPACE, ConnStrKeys.ALIAS],
        [ConnStrKeys.WORKSPACE, ConnStrKeys.APPKEY, ConnStrKeys.ALIAS],  # only for demo, if workspace = "DEMO_WORKSPACE"
    ]
    _DATA_SOURCE = "https://api.loganalytics.io"

    # Class methods
    # -------------

    @classmethod
    def tell_format(cls):
        return """
               loganalytics://workspace('workspaceid').appkey('appkey')

               ## Note: if appkey is missing, user will be prompted to enter appkey"""

    # Instance methods
    # ----------------

    def __init__(self, conn_str, current=None):
        super().__init__()
        self._parsed_conn = self._parse_common_connection_str(
            conn_str, current, self._URI_SCHEMA_NAME, self._MANDATORY_KEY, self._ALT_URI_SCHEMA_NAMES, self._VALID_KEYS_COMBINATIONS
        )
        self.client = DraftClient(self._parsed_conn, self._DOMAIN, self._DATA_SOURCE)

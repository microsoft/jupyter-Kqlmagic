#-------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
#--------------------------------------------------------------------------

import re
from kql.kql_engine import KqlEngine, KqlEngineError
from kql.draft_client import DraftClient
import getpass


class LoganalyticsEngine(KqlEngine):
    schema = "loganalytics://"
    _DOMAIN = "workspaces"
    _API_URI = "https://api.loganalytics.io"

    @classmethod
    def tell_format(cls):
        return """
               loganalytics://workspace('workspaceid').appkey('appkey')

               ## Note: if appkey is missing, user will be prompted to enter appkey"""

    # Object constructor
    def __init__(self, conn_str, current=None):
        super().__init__()
        schema = "loganalytics"
        mandatory_key = "workspace"
        not_in_url_key = None # "workspace"
        self._parse_common_connection_str(conn_str, current, schema, mandatory_key, not_in_url_key)
        self.client = DraftClient(self._parsed_conn, self._DOMAIN, self._API_URI)

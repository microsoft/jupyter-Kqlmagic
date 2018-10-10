#-------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
#--------------------------------------------------------------------------

import os.path
import re
from kql.kql_engine import KqlEngine, KqlEngineError
from kql.draft_client import DraftClient
import requests
import getpass


class AppinsightsEngine(KqlEngine):
    schema = "appinsights://"
    _DOMAIN = "apps"
    _API_URI = "https://api.applicationinsights.io"



    @classmethod
    def tell_format(cls):
        return """
               appinsights://appid('appid').appkey('appkey')

               ## Note: if appkey is missing, user will be prompted to enter appkey"""

    # Object constructor
    def __init__(self, conn_str, current=None):
        super().__init__()
        schema = "appinsights"
        mandatory_key = "appid"
        not_in_url_key = None # "appid"
        self._parse_common_connection_str(conn_str, current, schema, mandatory_key, not_in_url_key)
        self.client = DraftClient(self._parsed_conn, self._DOMAIN, self._API_URI)

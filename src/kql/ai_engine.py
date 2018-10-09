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
        self._parse_connection_str(conn_str, current)
        self.client = DraftClient(self._parsed_conn, self._DOMAIN, self._API_URI)

    def _parse_connection_str(self, conn_str: str, current):
        match = None
        # conn_str = "appinsights://appid('appinsight-appid').appkey('key')"
        if not match:
            pattern = re.compile(r"^appinsights://appid\((?P<appid>.*)\)\.appkey\((?P<appkey>.*)\)$")
            match = pattern.search(conn_str)
            if match:
                self._parsed_conn["appid"] = match.group("appid").strip()[1:-1]
                self._parsed_conn["appkey"] = match.group("appkey").strip()[1:-1]
                if self._parsed_conn["appkey"].lower() == "<appkey>":
                    self._parsed_conn["appkey"] = getpass.getpass(prompt="please enter appkey: ")

        if not match:
            pattern = re.compile(r"^appinsights://appid\((?P<appid>.*)\)$")
            match = pattern.search(conn_str)
            if match:
                self._parsed_conn["appid"] = match.group("appid").strip()[1:-1]
                self._parsed_conn["appkey"] = getpass.getpass(prompt="please enter appkey: ")

        if match:
            self.cluster_name = "appinsights"
            self.database_name = self._parsed_conn["appid"]
            self.bind_url = "appinsights://appid('{0}').appkey('{1}').cluster('{2}').database('{3}')".format(
                self._parsed_conn["appid"], self._parsed_conn["appkey"], self.cluster_name, self.database_name)
        else:
            schema = "appinsights"
            keys = ["tenant", "code", "clientid", "clientsecret", "appid"]
            mandatory_key = "appid"
            not_in_url_key = None # "appid"
            self._parse_common_connection_str(conn_str, current, schema, keys, mandatory_key, not_in_url_key)

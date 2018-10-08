#-------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
#--------------------------------------------------------------------------

import re
from kql.kql_engine import KqlEngine, KqlEngineError
from kql.la_client import LoganalyticsClient
import getpass


class LoganalyticsEngine(KqlEngine):
    schema = "loganalytics://"

    @classmethod
    def tell_format(cls):
        return """
               loganalytics://workspace('workspaceid').appkey('appkey')

               ## Note: if appkey is missing, user will be prompted to enter appkey"""

    # Object constructor
    def __init__(self, conn_str, current=None):
        super().__init__()
        self.parse_connection_str(conn_str, current)
        self.client = LoganalyticsClient(self._parsed_conn)

    def parse_connection_str(self, conn_str: str, current):
        match = None
        # conn_str = "loganalytics://workspace('workspace-id').appkey('key')"
        if not match:
            pattern = re.compile(r"^loganalytics://workspace\((?P<workspace>.*)\)\.appkey\((?P<appkey>.*)\)$")
            match = pattern.search(conn_str)
            if match:
                self._parsed_conn["workspace"] = match.group("workspace").strip()[1:-1]
                self._parsed_conn["appkey"] = match.group("appkey").strip()[1:-1]
                if self._parsed_conn["appkey"].lower() == "<appkey>":
                    self._parsed_conn["appkey"] = getpass.getpass(prompt="please enter appkey: ")

        if not match:
            pattern = re.compile(r"^loganalytics://workspace\((?P<workspace>.*)\)$")
            match = pattern.search(conn_str)
            if match:
                self._parsed_conn["workspace"] = match.group("workspace").strip()[1:-1]
                self._parsed_conn["appkey"] = getpass.getpass(prompt="please enter appkey: ")

        if match:
            self.cluster_name = "loganalytics"
            self.database_name = self._parsed_conn["workspace"]
            self.bind_url = "loganalytics://workspace('{0}').appkey('{1}').cluster('{2}').database('{3}')".format(
                self._parsed_conn["workspace"], self._parsed_conn["appkey"], self.cluster_name, self.database_name)
        else:
            schema = "loganalytics"
            keys = ["tenant", "code", "clientid", "clientsecret", "workspace"]
            mandatory_key = "workspace"
            not_in_url_key = "workspace"
            self._parse_common_connection_str(conn_str, current, schema, keys, mandatory_key, not_in_url_key)

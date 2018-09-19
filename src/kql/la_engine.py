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
        self.username = None
        self.password = None
        self.workspace = None
        self.appkey = None
        self.api_version = "v1"
        self.cluster_url = "https://api.applicationinsights.io/{0}/apps".format(self.api_version)
        self.parse_connection_str(conn_str, current)
        self.client = LoganalyticsClient(workspace=self.workspace, appkey=self.appkey, version=self.api_version)

    def parse_connection_str(self, conn_str: str, current):
        match = None
        # conn_str = "kusto://username('michabin@microsoft.com').password('g=Hh-h34G').cluster('Oiildc').database('OperationInsights_PFS_PROD')"
        if not match:
            pattern = re.compile(r"^loganalytics://workspace\((?P<workspace>.*)\)\.appkey\((?P<appkey>.*)\)$")
            match = pattern.search(conn_str)
            if match:
                self.workspace = match.group("workspace").strip()[1:-1]
                self.appkey = match.group("appkey").strip()[1:-1]
                if self.appkey.lower() == "<appkey>":
                    self.appkey = getpass.getpass(prompt="please enter appkey: ")

        if not match:
            pattern = re.compile(r"^loganalytics://workspace\((?P<workspace>.*)\)$")
            match = pattern.search(conn_str)
            if match:
                self.workspace = match.group("workspace").strip()[1:-1]
                self.appkey = getpass.getpass(prompt="please enter appkey: ")

        if not match:
            raise KqlEngineError("Invalid connection string.")

        self.cluster_name = "loganalytics"
        self.database_name = self.workspace
        self.bind_url = "loganalytics://workspace('{0}').appkey('{1}').cluster('{2}').database('{3}')".format(
            self.workspace, self.appkey, self.cluster_name, self.database_name
        )

import os.path
import re
from kql.kql_engine import KqlEngine, KqlEngineError
from kql.ai_client import AppinsightsClient
import requests
import getpass


class AppinsightsEngine(KqlEngine):
    schema = "appinsights://"

    @classmethod
    def tell_format(cls):
        return """
               appinsights://appid('appid').appkey('appkey')

               ## Note: if appkey is missing, user will be prompted to enter appkey"""

    # Object constructor
    def __init__(self, conn_str, current=None):
        super().__init__()
        self.api_version = "v1"
        self.username = None
        self.password = None
        self.appid = None
        self.appkey = None
        self.cluster_url = "https://api.applicationinsights.io/{0}/apps".format(self.api_version)
        self._parse_connection_str(conn_str, current)
        self.client = AppinsightsClient(appid=self.appid, appkey=self.appkey, version=self.api_version)

    def _parse_connection_str(self, conn_str: str, current):
        match = None
        # conn_str = "kusto://username('michabin@microsoft.com').password('g=Hh-h34G').cluster('Oiildc').database('OperationInsights_PFS_PROD')"
        if not match:
            pattern = re.compile(r"^appinsights://appid\((?P<appid>.*)\)\.appkey\((?P<appkey>.*)\)$")
            match = pattern.search(conn_str)
            if match:
                self.appid = match.group("appid").strip()[1:-1]
                self.appkey = match.group("appkey").strip()[1:-1]
                if self.appkey.lower() == "<appkey>":
                    self.appkey = getpass.getpass(prompt="please enter appkey: ")

        if not match:
            pattern = re.compile(r"^appinsights://appid\((?P<appid>.*)\)$")
            match = pattern.search(conn_str)
            if match:
                self.appid = match.group("appid").strip()[1:-1]
                self.database_name = self.appid
                self.appkey = getpass.getpass(prompt="please enter appkey: ")

        if not match:
            raise KqlEngineError("Invalid connection string.")

        self.cluster_name = "appinsights"
        self.database_name = self.appid
        self.bind_url = "appinsights://appid('{0}').appkey('{1}').cluster('{2}').database('{3}')".format(
            self.appid, self.appkey, self.cluster_name, self.database_name
        )

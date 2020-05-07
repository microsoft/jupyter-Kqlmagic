# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

import re
import uuid
import json


import requests


from .my_aad_helper import _MyAadHelper, ConnKeysKCSB
from .kql_response import KqlQueryResponse, KqlError
from .constants import Constants, ConnStrKeys, Cloud
from .version import VERSION
from .log import logger
from .kql_engine import KqlEngineError


class Kusto_Client(object):
    """
    Kusto client wrapper for Python."""
 
    _DEFAULT_CLIENTID = "db662dc1-0cfe-4e1c-a843-19a68e65be58"  # kusto client app, (didn't find app name ?)

    _MGMT_ENDPOINT_VERSION = "v1"
    _QUERY_ENDPOINT_VERSION = "v2"
    _MGMT_ENDPOINT_TEMPLATE = "{0}/{1}/rest/mgmt"
    _QUERY_ENDPOINT_TEMPLATE = "{0}/{1}/rest/query"


    _PUBLIC_CLOUD_URL_SUFFIX =      "windows.net"
    _MOONCAKE_CLOUD_URL_SUFFIX =    "chinacloudapi.cn"
    _BLACKFOREST_CLOUD_URL_SUFFIX = "cloudapi.de"
    _FAIRFAX_CLOUD_URL_SUFFIX =     "usgovcloudapi.net"



    _CLOUD_URLS = {
        Cloud.PUBLIC:      _PUBLIC_CLOUD_URL_SUFFIX,
        Cloud.MOONCAKE:    _MOONCAKE_CLOUD_URL_SUFFIX,
        Cloud.FAIRFAX:     _FAIRFAX_CLOUD_URL_SUFFIX,
        Cloud.BLACKFOREST: _BLACKFOREST_CLOUD_URL_SUFFIX
    }

    _DATA_SOURCE_TEMPLATE = "https://{0}.kusto.{1}"

    _WEB_CLIENT_VERSION = VERSION

    _FQN_DRAFT_PROXY_CLUSTER_PATTERN = re.compile(r"http(s?)\:\/\/ade\.(int\.)?(applicationinsights|loganalytics)\.(io|cn|us|de).*$")


    def __init__(self, conn_kv:dict, **options):
        """
        Kusto Client constructor.

        Parameters
        ----------
        kusto_cluster : str
            Kusto cluster endpoint. Example: https://help.kusto.windows.net
        client_id : str
            The AAD application ID of the application making the request to Kusto
        client_secret : str
            The AAD application key of the application making the request to Kusto.
            if this is given, then username/password should not be.
        username : str
            The username of the user making the request to Kusto.
            if this is given, then password must follow and the client_secret should not be given.
        password : str
            The password matching the username of the user making the request to Kusto
        authority : 'microsoft.com', optional
            In case your tenant is not microsoft please use this param.
        """
        self.cloud = options.get("cloud")
        cluster_name = conn_kv[ConnStrKeys.CLUSTER]

        if cluster_name.find("://") >= 0:
            data_source = cluster_name
        else:
            cloud_url = self._CLOUD_URLS.get(self.cloud)
            if not cloud_url:
                raise KqlEngineError(f"adx not supported in cloud {self.cloud}")
            data_source = self._DATA_SOURCE_TEMPLATE.format(cluster_name, cloud_url)

        self._mgmt_endpoint = self._MGMT_ENDPOINT_TEMPLATE.format(data_source, self._MGMT_ENDPOINT_VERSION)
        self._query_endpoint = self._QUERY_ENDPOINT_TEMPLATE.format(data_source, self._QUERY_ENDPOINT_VERSION)

        if self._FQN_DRAFT_PROXY_CLUSTER_PATTERN.match(data_source):
            auth_resource = f"https://kusto.kusto.{self._CLOUD_URLS.get(self.cloud)}"
        else:
            auth_resource = data_source
            
        self._aad_helper = _MyAadHelper(ConnKeysKCSB(conn_kv, auth_resource), self._DEFAULT_CLIENTID, **options) if conn_kv.get(ConnStrKeys.ANONYMOUS) is None else None
        self._data_source = data_source


    @property
    def data_source(self):
        return self._data_source


    @property 
    def deep_link_data_source(self):
        if self._FQN_DRAFT_PROXY_CLUSTER_PATTERN.match(self.data_source):
            return f"https://help.kusto.{self._CLOUD_URLS.get(self.cloud)}"
        else:
            return self._data_source


    def getCloudFromHTTP(self, http: str):
        if http.endswith(self._PUBLIC_CLOUD_URL_SUFFIX):
            return Cloud.PUBLIC
        if http.endswith(self._MOONCAKE_CLOUD_URL_SUFFIX):
            return Cloud.MOONCAKE
        if http.endswith(self._FAIRFAX_CLOUD_URL_SUFFIX):
            return Cloud.FAIRFAX
        if http.endswith(self._BLACKFOREST_CLOUD_URL_SUFFIX):
            return Cloud.BLACKFOREST
        return Cloud.PUBLIC


    def execute(self, kusto_database, kusto_query, accept_partial_results=False, **options):
        """ 
        Execute a simple query or management command

        Parameters
        ----------
        kusto_database : str
            Database against query will be executed.
        query : str
            Query to be executed
        accept_partial_results : bool
            Optional parameter. If query fails, but we receive some results, we consider results as partial.
            If this is True, results are returned to client, even if there are exceptions.
            If this is False, exception is raised. Default is False.
        options["timeout"] : float, optional
            Optional parameter. Network timeout in seconds. Default is no timeout.
        """
        if kusto_query.startswith("."):
            endpoint_version = self._MGMT_ENDPOINT_VERSION
            endpoint = self._mgmt_endpoint  
        else:
            endpoint_version = self._QUERY_ENDPOINT_VERSION
            endpoint = self._query_endpoint

        # print("### db: ", kusto_database, " ###")
        # print("### csl: ", kusto_query, " ###")
        # kusto_database = kusto_database.replace(" ", "")
        # print("### db: ", kusto_database, " ###")

        request_payload = {
            "db": kusto_database, 
            "csl": kusto_query,
        }

        client_version = f"{Constants.MAGIC_CLASS_NAME}.Python.Client:{self._WEB_CLIENT_VERSION}"

        client_request_id = f"{Constants.MAGIC_CLASS_NAME}.execute"
        client_request_id_tag = options.get("request_id_tag")
        if client_request_id_tag is not None:
            client_request_id = f"{client_request_id};{client_request_id_tag};{str(uuid.uuid4())}"
        else:
            client_request_id = f"{client_request_id};{str(uuid.uuid4())}"
            
        app = f'{Constants.MAGIC_CLASS_NAME};{options.get("notebook_app")}'
        app_tag = options.get("request_app_tag")
        if app_tag is not None:
            app = f"{app};{app_tag}"

        query_properties: dict = options.get("query_properties")
        if query_properties is not None and len(query_properties) > 0:
            properties = {
                "Options": query_properties,
                "Parameters": {},
                "ClientRequestId": client_request_id
            }
            request_payload["properties"] = json.dumps(properties)

        request_headers = {
            "Accept": "application/json",
            "Accept-Encoding": "gzip,deflate",
            "Content-Type": "application/json; charset=utf-8",
            "x-ms-client-version": client_version,
            "x-ms-client-request-id": client_request_id,
            "x-ms-app": app
        }
        user_tag = options.get("request_user_tag")
        if user_tag is not None:
            request_headers["x-ms-user"] = user_tag
        if self._aad_helper is not None:
            request_headers["Authorization"] = self._aad_helper.acquire_token()
            request_headers["Fed"] = "True"
        # print("endpoint: ", endpoint)
        # print("headers: ", request_headers)
        # print("payload: ", request_payload)
        # print("timeout: ", options.get("timeout"))

        log_request_headers = request_headers
        if request_headers.get("Authorization"):
            log_request_headers = request_headers.copy()
            log_request_headers["Authorization"] = "..."  

        logger().debug(f"Kusto_Client::execute - POST request - url: {endpoint}, headers: {log_request_headers}, payload: {request_payload}, timeout: options.get('timeout')")

        response = requests.post(endpoint, headers=request_headers, json=request_payload, timeout=options.get("timeout"))

        logger().debug(f"Kusto_Client::execute - response - status: {response.status_code}, headers: {response.headers}, payload: {response.text}")

        # print("response status code: ", response.status_code)
        # print("response", response)
        # print("response text", response.text)

        if response.status_code != requests.codes.ok:  # pylint: disable=E1101
            raise KqlError(response.text, response)

        kql_response = KqlQueryResponse(response.json(), endpoint_version)

        if kql_response.has_exceptions() and not accept_partial_results:
            try:
                error_message = json.dumps(kql_response.get_exceptions())
            except:
                error_message = str(kql_response.get_exceptions())
            raise KqlError(error_message, response, kql_response)

        return kql_response


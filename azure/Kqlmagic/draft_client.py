# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

import uuid
from datetime import timedelta, datetime
import json


import six
import adal
import dateutil.parser
import requests


from .constants import Constants, ConnStrKeys, Cloud, Schema
from .kql_response import KqlQueryResponse, KqlSchemaResponse, KqlError
from .my_aad_helper import _MyAadHelper, ConnKeysKCSB
from .version import VERSION
from .log import logger
from .kql_engine import KqlEngineError


class DraftClient(object):
    """Draft Client

        Parameters
        ----------
        conn_kv : dict
            Connection string key/value that contains the credentials to access the resource via Draft.
        domain: str
            The Draft client domain, either apps for the case of Application Insights or workspaces for the case Log Analytics.
        data_source: str
            The data source url.
    """

    #
    # Constants
    #

    _DEFAULT_CLIENTID = "db662dc1-0cfe-4e1c-a843-19a68e65be58"
    _WEB_CLIENT_VERSION = VERSION
    _API_VERSION = "v1"
    _GET_SCHEMA_QUERY = ".show schema"

    _CLOUD_AAD_URLS_APPINSIGHTS={
        Cloud.PUBLIC:      "https://api.applicationinsights.io",
        Cloud.MOONCAKE:    "https://api.applicationinsights.azure.cn",
        Cloud.FAIRFAX:     "https://api.applicationinsights.us",
        Cloud.BLACKFOREST: "https://api.applicationinsights.de",
    }

    _CLOUD_AAD_URLS_LOGANALYTICS={
        Cloud.PUBLIC:      "https://api.loganalytics.io",
        Cloud.MOONCAKE:    "https://api.loganalytics.azure.cn",
        Cloud.FAIRFAX:     "https://api.loganalytics.us",
        Cloud.BLACKFOREST: "https://api.loganalytics.de",
    }

    _CLOUD_AAD_URLS = {
        Schema.APPLICATION_INSIGHTS : _CLOUD_AAD_URLS_APPINSIGHTS,
        Schema.LOG_ANALYTICS : _CLOUD_AAD_URLS_LOGANALYTICS
    }


    def __init__(self, conn_kv: dict, domain: str, data_source: str, schema: str, **options):
        self._domain = domain

        if conn_kv.get(ConnStrKeys.DATA_SOURCE_URL):
            self._data_source =  conn_kv.get(ConnStrKeys.DATA_SOURCE_URL)  
            logger().debug(f"draft_client.py :: __init__ :  self._data_source from conn_kv[\"datasourceurl\"]: {self._data_source}")

        else:
            cloud = options.get("cloud")
            self._data_source = self._CLOUD_AAD_URLS.get(schema).get(cloud)
                
            if not self._data_source:
                raise KqlEngineError(f"the service {schema} is not supported in cloud {cloud}")

        logger().debug(f"draft_client.py :: __init__ :  conn_kv[\"datasourceurl\"]: {conn_kv.get(ConnStrKeys.DATA_SOURCE_URL)}")

        self._appkey = conn_kv.get(ConnStrKeys.APPKEY)
        logger().debug(f"draft_client.py :: __init__ :  self._appkey: {self._appkey}")


        if self._appkey is None and conn_kv.get(ConnStrKeys.ANONYMOUS) is None:
            self._aad_helper = _MyAadHelper(ConnKeysKCSB(conn_kv, self._data_source), self._DEFAULT_CLIENTID, **options)
        else:
            self._aad_helper = None
        logger().debug(f"draft_client.py :: __init__ :  self._aad_helper: {self._aad_helper} ;")


    @property
    def data_source(self):
        return self._data_source


    def execute(self, id: str, query: str, accept_partial_results: bool = False, **options) -> object:
        """ Execute a simple query or a metadata query
        
        Parameters
        ----------
        id : str
            the workspaces (log analytics) or appid (application insights).
        query : str
            Query to be executed
        accept_partial_results : bool, optional
            Optional parameter. If query fails, but we receive some results, we consider results as partial.
            If this is True, results are returned to client, even if there are exceptions.
            If this is False, exception is raised. Default is False.
        oprions["timeout"] : float, optional
            Optional parameter. Network timeout in seconds. Default is no timeout.

        Returns
        -------
        object
            KqlQueryResponse instnace if executed simple query request
            KqlSchemaResponse instnace if executed metadata request

        Raises
        ------
        KqlError
            If request to draft failed.
            If response from draft contains exceptions.
        """

        #
        # create API url
        #

        is_metadata = query == self._GET_SCHEMA_QUERY
        api_url = f"{self._data_source}/{self._API_VERSION}/{self._domain}/{id}/{'metadata' if is_metadata else 'query'}"

        #
        # create Prefer header
        #

        prefer_list = []
        if self._API_VERSION != "beta":
            prefer_list.append("ai.response-thinning=false")  # returns data as kusto v1
        
        timeout = options.get("timeout")
        if timeout is not None:
            prefer_list.append(f"wait={timeout}")

        #
        # create headers
        #
        
        client_version = f"{Constants.MAGIC_CLASS_NAME}.Python.Client:{self._WEB_CLIENT_VERSION}"        

        client_request_id = f"{Constants.MAGIC_CLASS_NAME}.execute"
        client_request_id_tag = options.get("request_id_tag")
        if client_request_id_tag is not None:
            client_request_id = f"{client_request_id};{client_request_id_tag};{str(uuid.uuid4())}"
        else:
            client_request_id = f"{client_request_id};{str(uuid.uuid4())}"

        app = f"{Constants.MAGIC_CLASS_NAME}"
        app_tag = options.get("request_app_tag")
        if app_tag is not None:
            app = f"{app};{app_tag}"

        request_headers = {
            "x-ms-client-version": client_version,
            "x-ms-client-request-id": client_request_id,
            "x-ms-app": app
        }
        user_tag = options.get("request_user_tag")
        if user_tag is not None:
            request_headers["x-ms-user"] = user_tag

        if self._aad_helper is not None:
            request_headers["Authorization"] = self._aad_helper.acquire_token(**options)
        elif self._appkey is not None:
            request_headers["x-api-key"] = self._appkey

        if len(prefer_list) > 0:
            request_headers["Prefer"] = ", ".join(prefer_list)

        #
        # submit request
        #
        log_request_headers = request_headers
        if request_headers.get("Authorization"):
            log_request_headers = request_headers.copy()
            log_request_headers["Authorization"] = "..." 

        if is_metadata:
            logger().debug("DraftClient::execute - GET request - url: %s, headers: %s, timeout: %s", api_url, log_request_headers, options.get("timeout"))
            response = requests.get(api_url, headers=request_headers)
        else:
            request_payload = {"query": query}
            logger().debug("DraftClient::execute - POST request - url: %s, headers: %s, payload: %s, timeout: %s", api_url, log_request_headers, request_payload, options.get("timeout"))
            response = requests.post(api_url, headers=request_headers, json=request_payload)

        logger().debug("DraftClient::execute - response - status: %s, headers: %s, payload: %s", response.status_code, response.headers, response.text)
        #
        # handle response
        #

        if response.status_code != requests.codes.ok:  # pylint: disable=E1101
            raise KqlError(response.text, response)

        json_response = response.json()

        if is_metadata:
            kql_response = KqlSchemaResponse(json_response)
        else:
            kql_response = KqlQueryResponse(json_response)

        if kql_response.has_exceptions() and not accept_partial_results:
            try:
                error_message = json.dumps(kql_response.get_exceptions())
            except:
                error_message = str(kql_response.get_exceptions())
            raise KqlError(error_message, response, kql_response)

        return kql_response


# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

from typing import Union, Dict
import uuid
import json


from .my_utils import json_dumps 
from .constants import Constants, ConnStrKeys, Cloud, Schema
from .kql_response import KqlQueryResponse, KqlSchemaResponse, KqlError
# from .my_aad_helper import _MyAadHelper, ConnKeysKCSB
from .my_aad_helper_msal import _MyAadHelper, ConnKeysKCSB
from ._version import __version__
from .log import logger
from .kql_client import KqlClient
from .exceptions import KqlEngineError

class DraftClient(KqlClient):
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


    _DRAFT_CLIENT_BY_CLOUD = {
        Cloud.PUBLIC:      "db662dc1-0cfe-4e1c-a843-19a68e65be58",
        Cloud.MOONCAKE:    "db662dc1-0cfe-4e1c-a843-19a68e65be58",
        Cloud.FAIRFAX:     "730ea9e6-1e1d-480c-9df6-0bb9a90e1a0f",
        Cloud.BLACKFOREST: "db662dc1-0cfe-4e1c-a843-19a68e65be58",
        Cloud.PPE:         "db662dc1-0cfe-4e1c-a843-19a68e65be58",
    }
    _DRAFT_CLIENT_BY_CLOUD[Cloud.CHINA]      = _DRAFT_CLIENT_BY_CLOUD[Cloud.MOONCAKE]
    _DRAFT_CLIENT_BY_CLOUD[Cloud.GOVERNMENT] = _DRAFT_CLIENT_BY_CLOUD[Cloud.FAIRFAX]
    _DRAFT_CLIENT_BY_CLOUD[Cloud.GERMANY]    = _DRAFT_CLIENT_BY_CLOUD[Cloud.BLACKFOREST]

    _WEB_CLIENT_VERSION = __version__
    _API_VERSION = "v1"
    _GET_SCHEMA_QUERY = ".show schema"

    _APPINSIGHTS_URL_BY_CLOUD = {
        Cloud.PUBLIC:      "https://api.applicationinsights.io",
        Cloud.MOONCAKE:    "https://api.applicationinsights.azure.cn",
        Cloud.FAIRFAX:     "https://api.applicationinsights.us",
        Cloud.BLACKFOREST: "https://api.applicationinsights.de",
    }
    _APPINSIGHTS_URL_BY_CLOUD[Cloud.CHINA]      = _APPINSIGHTS_URL_BY_CLOUD[Cloud.MOONCAKE]
    _APPINSIGHTS_URL_BY_CLOUD[Cloud.GOVERNMENT] = _APPINSIGHTS_URL_BY_CLOUD[Cloud.FAIRFAX]
    _APPINSIGHTS_URL_BY_CLOUD[Cloud.GERMANY]    = _APPINSIGHTS_URL_BY_CLOUD[Cloud.BLACKFOREST]

    _LOGANALYTICS_URL_BY_CLOUD = {
        Cloud.PUBLIC:      "https://api.loganalytics.io",
        Cloud.MOONCAKE:    "https://api.loganalytics.azure.cn",
        Cloud.FAIRFAX:     "https://api.loganalytics.us",
        Cloud.BLACKFOREST: "https://api.loganalytics.de",
    }
    _LOGANALYTICS_URL_BY_CLOUD[Cloud.CHINA]      = _LOGANALYTICS_URL_BY_CLOUD[Cloud.MOONCAKE]
    _LOGANALYTICS_URL_BY_CLOUD[Cloud.GOVERNMENT] = _LOGANALYTICS_URL_BY_CLOUD[Cloud.FAIRFAX]
    _LOGANALYTICS_URL_BY_CLOUD[Cloud.GERMANY]    = _LOGANALYTICS_URL_BY_CLOUD[Cloud.BLACKFOREST]

    _DRAFT_URLS_BY_SCHEMA = {
        Schema.APPLICATION_INSIGHTS: _APPINSIGHTS_URL_BY_CLOUD,
        Schema.LOG_ANALYTICS:        _LOGANALYTICS_URL_BY_CLOUD
    }


    def __init__(self, conn_kv:Dict[str,str], domain:str, data_source:str, schema:str, **options)->None:
        super(DraftClient, self).__init__()
        self._domain = domain
        self.resources_name = "workspaces" if schema == Schema.LOG_ANALYTICS else "applications"

        if conn_kv.get(ConnStrKeys.DATA_SOURCE_URL):
            self._data_source =  conn_kv.get(ConnStrKeys.DATA_SOURCE_URL)  
            logger().debug(f"draft_client.py :: __init__ :  self._data_source from conn_kv[\"datasourceurl\"]: {self._data_source}")

        else:
            cloud = options.get("cloud")
            urls = self._DRAFT_URLS_BY_SCHEMA.get(schema, {})
            self._data_source = urls.get(cloud, data_source)
                
            if not self._data_source:
                raise KqlEngineError(f"the service {schema} is not supported in cloud {cloud}")

        logger().debug(f"draft_client.py :: __init__ :  conn_kv[\"datasourceurl\"]: {conn_kv.get(ConnStrKeys.DATA_SOURCE_URL)}")

        self._appkey = conn_kv.get(ConnStrKeys.APPKEY)
        logger().debug(f"draft_client.py :: __init__ :  self._appkey: {self._appkey}")


        if self._appkey is None and conn_kv.get(ConnStrKeys.ANONYMOUS) is None:
            cloud = options.get("cloud")
            client_id = self._DRAFT_CLIENT_BY_CLOUD[cloud]
            http_client = self._http_client if options.get("auth_use_http_client") else None
            self._aad_helper = _MyAadHelper(ConnKeysKCSB(conn_kv, self._data_source), client_id, http_client=http_client, **options)
        else:
            self._aad_helper = None
        logger().debug(f"draft_client.py :: __init__ :  self._aad_helper: {self._aad_helper} ;")


    @property
    def data_source(self)->str:
        return self._data_source


    def execute(self, id:str, query:str, accept_partial_results:bool=False, **options)->Union[KqlQueryResponse, KqlSchemaResponse]:
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
            client_request_id = f"{client_request_id};{client_request_id_tag};{str(uuid.uuid4())}/{self._session_guid}/AzureMonitor"
        else:
            client_request_id = f"{client_request_id};{str(uuid.uuid4())}/{self._session_guid}/AzureMonitor"

        app = f'{Constants.MAGIC_CLASS_NAME};{options.get("notebook_app")}'
        app_tag = options.get("request_app_tag")
        if app_tag is not None:
            app = f"{app};{app_tag}"
            
        user_agent = f'{Constants.MAGIC_CLASS_NAME}/{self._WEB_CLIENT_VERSION}'
        user_agent_tag = options.get("request_user_agent_tag")
        if user_agent_tag is not None:
            user_agent = user_agent_tag

        request_headers = {
            "User-Agent": user_agent,
            "x-ms-client-version": client_version,
            "x-ms-client-request-id": client_request_id,
            "x-ms-app": app
        }
        user_tag = options.get("request_user_tag")
        if user_tag is not None:
            request_headers["x-ms-user"] = user_tag

        if self._aad_helper is not None:
            request_headers["Authorization"] = self._aad_helper.acquire_token()
        elif self._appkey is not None:
            request_headers["x-api-key"] = self._appkey

        if len(prefer_list) > 0:
            request_headers["Prefer"] = ", ".join(prefer_list)

        cache_max_age = options.get("request_cache_max_age")
        if cache_max_age is not None:
            if cache_max_age > 0:
                request_headers["Cache-Control"] = f"max-age={cache_max_age}"
            else:
                request_headers["Cache-Control"] = "no-cache"

        #
        # submit request
        #
        log_request_headers = request_headers
        if request_headers.get("Authorization"):
            log_request_headers = request_headers.copy()
            log_request_headers["Authorization"] = "..." 

        # collect this inormation, in case bug report will be generated
        KqlClient.last_query_info = {
            "request": {
                "endpoint": api_url,
                "headers": log_request_headers,
                "timeout": options.get("timeout"),
            }
        }

        if is_metadata:
            logger().debug(f"DraftClient::execute - GET request - url: {api_url}, headers: {log_request_headers}, timeout: {options.get('timeout')}")
            response = self._http_client.get(api_url, headers=request_headers, timeout=options.get("timeout"))
        else:
            request_payload = {
                "query": query
            }

            # Implicit Cross Workspace Queries: https://dev.loganalytics.io/oms/documentation/3-Using-the-API/CrossResourceQuery
            # workspaces - string[] - A list of workspaces that are included in the query.
            if type(options.get("query_properties")) == dict:
                resources = options.get("query_properties").get(self.resources_name)
                if type(resources) == list and len(resources) > 0:
                    request_payload[self.resources_name] = resources

                timespan = options.get("query_properties").get("timespan")
                if type(timespan) == str and len(timespan) > 0:
                    request_payload["timespan"] = timespan

            logger().debug(f"DraftClient::execute - POST request - url: {api_url}, headers: {log_request_headers}, payload: {request_payload}, timeout: {options.get('timeout')}")

            # collect this inormation, in case bug report will be generated
            self.last_query_info["request"]["payload"] = request_payload  # pylint: disable=unsupported-assignment-operation, unsubscriptable-object

            response = self._http_client.post(api_url, headers=request_headers, json=request_payload, timeout=options.get("timeout"))

        logger().debug(f"DraftClient::execute - response - status: {response.status_code}, headers: {response.headers}, payload: {response.text}")
        #
        # handle response
        #
        # collect this inormation, in case bug report will be generated
        self.last_query_info["response"] = {  # pylint: disable=unsupported-assignment-operation
            "status_code": response.status_code
        }

        if response.status_code < 200  or response.status_code >= 300:  # pylint: disable=E1101
            try:
                parsed_error = json.loads(response.text)
            except:
                parsed_error = response.text
            # collect this inormation, in case bug report will be generated
            self.last_query_info["response"]["error"] = parsed_error  # pylint: disable=unsupported-assignment-operation, unsubscriptable-object
            raise KqlError(response.text, response)

        json_response = response.json()

        if is_metadata:
            kql_response = KqlSchemaResponse(json_response)
        else:
            kql_response = KqlQueryResponse(json_response)

        if kql_response.has_exceptions() and not accept_partial_results:
            try:
                error_message = json_dumps(kql_response.get_exceptions())
            except:
                error_message = str(kql_response.get_exceptions())
            raise KqlError(error_message, response, kql_response)

        return kql_response

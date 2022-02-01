# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

from typing import Dict
import re
import uuid
import json


from .my_aad_helper_msal import _MyAadHelper, ConnKeysKCSB
from .kql_response import KqlQueryResponse, KqlError
from .constants import Constants, ConnStrKeys, Cloud
from ._version import __version__
from .log import logger
from .exceptions import KqlEngineError
from .my_utils import json_dumps 
from .kql_client import KqlClient


class KustoClient(KqlClient):
    """
    Kusto client wrapper for Python."""
 
    _ADX_CLIENT_BY_CLOUD = {
        Cloud.PUBLIC:      "db662dc1-0cfe-4e1c-a843-19a68e65be58",
        Cloud.MOONCAKE:    "db662dc1-0cfe-4e1c-a843-19a68e65be58",
        Cloud.FAIRFAX:     "730ea9e6-1e1d-480c-9df6-0bb9a90e1a0f",
        Cloud.BLACKFOREST: "db662dc1-0cfe-4e1c-a843-19a68e65be58",
        Cloud.PPE:         "db662dc1-0cfe-4e1c-a843-19a68e65be58",
    }
    _ADX_CLIENT_BY_CLOUD[Cloud.CHINA]      = _ADX_CLIENT_BY_CLOUD[Cloud.MOONCAKE]
    _ADX_CLIENT_BY_CLOUD[Cloud.GOVERNMENT] = _ADX_CLIENT_BY_CLOUD[Cloud.FAIRFAX]
    _ADX_CLIENT_BY_CLOUD[Cloud.GERMANY]    = _ADX_CLIENT_BY_CLOUD[Cloud.BLACKFOREST]

    _MGMT_ENDPOINT_VERSION = "v1"
    _QUERY_ENDPOINT_VERSION = "v2"
    _MGMT_ENDPOINT_TEMPLATE = "{0}/{1}/rest/mgmt"
    _QUERY_ENDPOINT_TEMPLATE = "{0}/{1}/rest/query"


    _ADX_PUBLIC_CLOUD_URL_SUFFIX      = ".windows.net"
    _ADX_MOONCAKE_CLOUD_URL_SUFFIX    = ".chinacloudapi.cn"
    _ADX_BLACKFOREST_CLOUD_URL_SUFFIX = ".cloudapi.de"
    _ADX_FAIRFAX_CLOUD_URL_SUFFIX     = ".usgovcloudapi.net"

    _CLOUD_BY_ADX_HOST_SUFFIX = {
        _ADX_PUBLIC_CLOUD_URL_SUFFIX:      Cloud.PUBLIC,
        _ADX_FAIRFAX_CLOUD_URL_SUFFIX:     Cloud.FAIRFAX,
        _ADX_MOONCAKE_CLOUD_URL_SUFFIX:    Cloud.MOONCAKE,
        _ADX_BLACKFOREST_CLOUD_URL_SUFFIX: Cloud.BLACKFOREST
    }


    _ADX_URL_SUFFIX_BY_CLOUD = {
        Cloud.PUBLIC:      _ADX_PUBLIC_CLOUD_URL_SUFFIX,
        Cloud.MOONCAKE:    _ADX_MOONCAKE_CLOUD_URL_SUFFIX,
        Cloud.FAIRFAX:     _ADX_FAIRFAX_CLOUD_URL_SUFFIX,
        Cloud.BLACKFOREST: _ADX_BLACKFOREST_CLOUD_URL_SUFFIX
    }
    _ADX_URL_SUFFIX_BY_CLOUD[Cloud.CHINA]      = _ADX_URL_SUFFIX_BY_CLOUD[Cloud.MOONCAKE]
    _ADX_URL_SUFFIX_BY_CLOUD[Cloud.GOVERNMENT] = _ADX_URL_SUFFIX_BY_CLOUD[Cloud.FAIRFAX]
    _ADX_URL_SUFFIX_BY_CLOUD[Cloud.GERMANY]    = _ADX_URL_SUFFIX_BY_CLOUD[Cloud.BLACKFOREST]


    _DATA_SOURCE_TEMPLATE = "https://{0}.kusto{1}"

    _WEB_CLIENT_VERSION = __version__

    _FQN_DRAFT_PROXY_CLUSTER_PATTERN = re.compile(r"http(s?)\:\/\/ade\.(int\.)?(applicationinsights|loganalytics)\.(?P<host_suffix>(io|cn|us|de)).*$")
    _FQN_DRAFT_PROXY_CLUSTER_PATTERN2 = re.compile(r"http(s?)\:\/\/adx\.(int\.)?monitor\.azure\.(?P<host_suffix>(com|cn|us|de)).*$")

    _CLOUD_BY_ADXPROXY_HOST_SUFFIX = {
        "com": Cloud.PUBLIC,
        "io":  Cloud.PUBLIC,
        "us":  Cloud.FAIRFAX,
        "cn":  Cloud.MOONCAKE,
        "de":  Cloud.BLACKFOREST
    }


    def __init__(self, cluster_name:str, conn_kv:Dict[str,str], **options)->None:
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

        super(KustoClient, self).__init__()
        self.default_cloud = options.get("cloud")
        cluster_name = cluster_name or conn_kv[ConnStrKeys.CLUSTER]

        if cluster_name.find("://") > 0:
            data_source = cluster_name
        elif cluster_name.find(".kusto.") > 0:
            data_source = f"https://{cluster_name}"
        elif cluster_name.find(".kusto(mfa).") > 0:
            data_source = f"https://{cluster_name}"
        elif cluster_name.find(".kustomfa.") > 0:
            data_source = f"https://{cluster_name}"
        else:
            adx_url_suffix = self._ADX_URL_SUFFIX_BY_CLOUD.get(self.default_cloud)
            if not adx_url_suffix:
                raise KqlEngineError(f"adx not supported in cloud {self.default_cloud}")
            if cluster_name.endswith(adx_url_suffix):
                data_source = f"https://{cluster_name}"
            else:
                data_source = self._DATA_SOURCE_TEMPLATE.format(cluster_name, adx_url_suffix)

        self._mgmt_endpoint = self._MGMT_ENDPOINT_TEMPLATE.format(data_source, self._MGMT_ENDPOINT_VERSION)
        self._query_endpoint = self._QUERY_ENDPOINT_TEMPLATE.format(data_source, self._QUERY_ENDPOINT_VERSION)

        match = self._FQN_DRAFT_PROXY_CLUSTER_PATTERN.match(data_source) or self._FQN_DRAFT_PROXY_CLUSTER_PATTERN2.match(data_source)
        if match:
            cloud = self._CLOUD_BY_ADXPROXY_HOST_SUFFIX.get(match.group("host_suffix")) or self.default_cloud
            cloud_url_suffix = self._ADX_URL_SUFFIX_BY_CLOUD.get(cloud)
            auth_resource = f"https://kusto.kusto{cloud_url_suffix}"
        else:
            auth_resource = data_source
        
        cloud = self.getCloudFromHost(auth_resource)
        client_id = self._ADX_CLIENT_BY_CLOUD[cloud]
        http_client = self._http_client if options.get("auth_use_http_client") else None
        self._aad_helper = _MyAadHelper(ConnKeysKCSB(conn_kv, auth_resource), client_id, http_client=http_client, **options) if conn_kv.get(ConnStrKeys.ANONYMOUS) is None else None
        self._data_source = data_source


    @property
    def data_source(self)->str:
        return self._data_source


    @property 
    def deep_link_data_source(self)->str:
        match = self._FQN_DRAFT_PROXY_CLUSTER_PATTERN.match(self.data_source) or self._FQN_DRAFT_PROXY_CLUSTER_PATTERN2.match(self.data_source)
        if match:
            cloud = self._CLOUD_BY_ADXPROXY_HOST_SUFFIX.get(match.group("host_suffix")) or self.default_cloud
            cloud_url_suffix = self._ADX_URL_SUFFIX_BY_CLOUD.get(cloud)
            return f"https://help.kusto{cloud_url_suffix}"
        else:
            return self._data_source


    def getCloudFromHost(self, host:str)->str:
        for adx_host_suffix in self._CLOUD_BY_ADX_HOST_SUFFIX:
            if host.endswith(adx_host_suffix):
                return self._CLOUD_BY_ADX_HOST_SUFFIX[adx_host_suffix]
        return Cloud.PUBLIC


    def execute(self, kusto_database:str, kusto_query:str, accept_partial_results:bool=False, **options)->KqlQueryResponse:
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
            client_request_id = f"{client_request_id};{client_request_id_tag};{str(uuid.uuid4())}/{self._session_guid}/AzureDataExplorer"
        else:
            client_request_id = f"{client_request_id};{str(uuid.uuid4())}/{self._session_guid}/AzureDataExplorer"
            
        app = f'{Constants.MAGIC_CLASS_NAME};{options.get("notebook_app")}'
        app_tag = options.get("request_app_tag")
        if app_tag is not None:
            app = f"{app};{app_tag}"

        query_properties:dict = options.get("query_properties")  or {}

        if type(kusto_query) == str:
            first_word = kusto_query.split(maxsplit=1)[0].upper()
            # ADX SQL mode
            if first_word in ["SELECT", "UPDATE", "CREATE", "DELETE", "EXPLAIN"]:
                # SQL to Kusto cheat sheet: https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/sqlcheatsheet
                # MS-TDS/T-SQL Differences between Kusto Microsoft SQL Server: https://docs.microsoft.com/en-us/azure/data-explorer/kusto/api/tds/sqlknownissues
                query_properties["query_language"] = "sql"

        cache_max_age = options.get("request_cache_max_age")
        if cache_max_age is not None and cache_max_age > 0:
            query_properties["query_results_cache_max_age"] = query_properties.get("query_results_cache_max_age")\
                                                              or f"{cache_max_age}s"

        if len(query_properties) > 0:
            properties = {
                "Options": query_properties,
                "Parameters": {},
                "ClientRequestId": client_request_id
            }
            request_payload["properties"] = json_dumps(properties)

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

        cache_max_age = options.get("request_cache_max_age")
        if cache_max_age is not None:
            if cache_max_age > 0:
                request_headers["Cache-Control"] = f"max-age={cache_max_age}"
            else:
                request_headers["Cache-Control"] = "no-cache"
            
        # print("endpoint: ", endpoint)
        # print("headers: ", request_headers)
        # print("payload: ", request_payload)
        # print("timeout: ", options.get("timeout"))

        log_request_headers = request_headers
        if request_headers.get("Authorization"):
            log_request_headers = request_headers.copy()
            log_request_headers["Authorization"] = "..."  

        logger().debug(f"KustoClient::execute - POST request - url: {endpoint}, headers: {log_request_headers}, payload: {request_payload}, timeout: {options.get('timeout')}")

        # collect this information, in case bug report will be generated
        KqlClient.last_query_info = {
            "request": {
                "endpoint": endpoint,
                "headers": log_request_headers,
                "payload": request_payload,
                "timeout": options.get("timeout"),
            }
        }

        response = self._http_client.post(endpoint, headers=request_headers, json=request_payload, timeout=options.get("timeout"))

        logger().debug(f"KustoClient::execute - response - status: {response.status_code}, headers: {response.headers}, payload: {response.text}")

        # print("response status code: ", response.status_code)
        # print("response", response)
        # print("response text", response.text)

        # collect this information, in case bug report will be generated
        self.last_query_info["response"] = {  # pylint: disable=unsupported-assignment-operation
            "status_code": response.status_code
        }

        if response.status_code < 200  or response.status_code >= 300:  # pylint: disable=E1101
            try:
                parsed_error = json.loads(response.text)
            except:
                parsed_error = response.text
            # collect this information, in case bug report will be generated
            self.last_query_info["response"]["error"] = parsed_error  # pylint: disable=unsupported-assignment-operation, unsubscriptable-object
            raise KqlError(response.text, response)

        kql_response = KqlQueryResponse(response.json(), endpoint_version)

        if kql_response.has_exceptions() and not accept_partial_results:
            try:
                error_message = json_dumps(kql_response.get_exceptions())
            except:
                error_message = str(kql_response.get_exceptions())
            raise KqlError(error_message, response, kql_response)

        return kql_response

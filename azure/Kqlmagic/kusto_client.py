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
from .kql_response import KqlQueryResponse, KqlError, KqlQueryResponse_CSV
from .constants import Constants, ConnStrKeys, Cloud
from .version import VERSION
from .log import logger
from .kql_engine import KqlEngineError


class Kusto_Client(object):
    """
    Kusto client wrapper for Python.

    KustoClient works with both 2.x and 3.x flavors of Python. All primitive types are supported.
    KustoClient takes care of ADAL authentication, parsing response and giving you typed result set,
    and offers familiar Python DB API.

    Test are run using nose.

    Examples
    --------
    To use KustoClient, you can choose betwen two ways of authentication.
     
    For the first option, you'll need to have your own AAD application and know your client credentials (client_id and client_secret).
    >>> kusto_cluster = 'https://help.kusto.windows.net'
    >>> kusto_client = KustoClient(kusto_cluster, client_id, client_secret='your_app_secret')

    For the second option, you can use KustoClient's client id and authenticate using your username and password.
    >>> kusto_cluster = 'https://help.kusto.windows.net'
    >>> client_id = 'e07cf1fb-c6a6-4668-b21a-f74731afa19a'
    >>> kusto_client = KustoClient(kusto_cluster, client_id, username='your_username', password='your_password')"""

    _DEFAULT_CLIENTID = "db662dc1-0cfe-4e1c-a843-19a68e65be58"  # kusto client app, (didn't find app name ?)
    #    _DEFAULT_CLIENTID = "8430759c-5626-4577-b151-d0755f5355d8" # kusto client app, don't know app name
    _MGMT_ENDPOINT_VERSION = "v1"
    _QUERY_ENDPOINT_VERSION = "v2"
    _MGMT_ENDPOINT_TEMPLATE = "{0}/{1}/rest/mgmt"
    _QUERY_ENDPOINT_TEMPLATE = "{0}/{1}/rest/query"


    _PUBLIC_CLOUD_URL_SUFFIX =      "windows.net"
    _MOONCAKE_CLOUD_URL_SUFFIX =    "chinacloudapi.cn"
    _BLACKFOREST_CLOUD_URL_SUFFIX = "cloudapi.de"
    _FAIRFAX_CLOUD_URL_SUFFIX =     "usgovcloudapi.net"
    _USNAT_CLOUD_URL_SUFFIX =       "core.eaglex.ic.gov"
    _USSEC_CLOUD_URL_SUFFIX =       "core.microsoft.scloud"


    _CLOUD_URLS = {
        Cloud.PUBLIC:      _PUBLIC_CLOUD_URL_SUFFIX,
        Cloud.MOONCAKE:    _MOONCAKE_CLOUD_URL_SUFFIX,
        Cloud.FAIRFAX:     _FAIRFAX_CLOUD_URL_SUFFIX,
        Cloud.BLACKFOREST: _BLACKFOREST_CLOUD_URL_SUFFIX,
        Cloud.USNAT:       _USNAT_CLOUD_URL_SUFFIX,
        Cloud.USSEC:       _USSEC_CLOUD_URL_SUFFIX
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
        if http.endswith(self._USNAT_CLOUD_URL_SUFFIX):
            return Cloud.USNAT
        if http.endswith(self._USSEC_CLOUD_URL_SUFFIX):
            return Cloud.USSEC
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
            
        app = f"{Constants.MAGIC_CLASS_NAME}"
        app_tag = options.get("request_app_tag")
        if app_tag is not None:
            app = f"{app};{app_tag}"

        query_properties: dict = options.get("query_properties")
        if query_properties and len(query_properties):
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
            request_headers["Authorization"] = self._aad_helper.acquire_token(**options)
            request_headers["Fed"] = "True"
        # print("endpoint: ", endpoint)
        # print("headers: ", request_headers)
        # print("payload: ", request_payload)
        # print("timeout: ", options.get("timeout"))

        log_request_headers = request_headers
        if request_headers.get("Authorization"):
            log_request_headers = request_headers.copy()
            log_request_headers["Authorization"] = "..."  

        logger().debug("Kusto_Client::execute - POST request - url: %s, headers: %s, payload: %s, timeout: %s", endpoint, log_request_headers, request_payload, options.get("timeout"))

        streaming_data = options.get("data_stream")
        response = requests.post(endpoint, headers=request_headers, json=request_payload, timeout=options.get("timeout"), stream=streaming_data)
        if response.status_code != requests.codes.ok:  # pylint: disable=E1101
            raise KqlError(response.text, response)

        if streaming_data:
            kql_response = stream_data_to_csv(response, endpoint_version)
        else:
        kql_response = KqlQueryResponse(response.json(), endpoint_version)

        if kql_response.has_exceptions() and not accept_partial_results:
            try:
                error_message = json.dumps(kql_response.get_exceptions())
            except:
                error_message = str(kql_response.get_exceptions())
            raise KqlError(error_message, response, kql_response)

        return kql_response

def stream_data_to_csv(response, endpoint_version): #writes to a few csv files the data returnd from Kusto. 
    import ijson
    from .Kql_response_wrapper import ResponseStream
    import csv


    stream = ResponseStream(response.iter_content())
    json_response = ijson.items(stream, '')
    header_table = [
        'FrameType' ,'TableId', 'TableKind' , 'TableName' , 'Columns' , 'Rows'
    ]
    header_table_dataset_completion = [
        'FrameType' ,'HasErrors', 'Cancelled'
    ]
    logger().debug(f"Kusto_Client::execute - header_table {header_table}, header_table_dataset_completion {header_table_dataset_completion}")

    with open("DataTable.csv", 'w', newline='') as f1,open("DataSetCompletion.csv", 'w', newline='') as f2,  open("PrimaryResult.csv", 'w', newline='') as f3, open("Tables_V1.csv", 'w', newline='') as f4:
        csv_write_all_tables = csv.DictWriter(f1,fieldnames=header_table)
        csv_write_dataset = csv.DictWriter(f2,fieldnames=header_table_dataset_completion)
        csv_write_primary = csv.DictWriter(f3,fieldnames=header_table)
        csv_write_v1 = csv.DictWriter(f4,fieldnames=['Tables'])

        csv_write_dataset.writeheader()
        csv_write_all_tables.writeheader()
        csv_write_primary.writeheader()
        csv_write_v1.writeheader()

        for big_item in json_response:
            if endpoint_version=="v2":
                logger().debug(f"Kusto_Client::after endpoint is v2")
                for item in big_item:
                    logger().debug(f"Kusto_Client::item in big_item is {item}")
                    if item["FrameType"] == "DataTable":
                        if item["TableKind"]=="PrimaryResult":
                            csv_write_primary.writerow(item)
                        csv_write_all_tables.writerow(item)
                    if item["FrameType"] == "DataSetCompletion":
                        csv_write_dataset.writerow(item)

            if endpoint_version=="v1":
                logger().debug(f"Kusto_Client::after endpoint is v1")
                csv_write_v1.writerow(big_item)
                logger().debug(f"Kusto_Client:: after csf write v1")

    return KqlQueryResponse_CSV(json_response, endpoint_version )

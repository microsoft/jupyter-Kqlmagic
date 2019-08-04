# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

import uuid
import six
from datetime import timedelta, datetime
import json
import adal
import dateutil.parser
import requests

from Kqlmagic.constants import Constants, ConnStrKeys
from Kqlmagic.kql_client import KqlQueryResponse, KqlSchemaResponse, KqlError
from Kqlmagic.my_aad_helper import _MyAadHelper, ConnKeysKCSB
from Kqlmagic.version import VERSION
from Kqlmagic.log import logger


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

    _CLOUD_AAD_URLS={
    "public": "https://api.{0}.io",
    "mooncake":"https://api.{0}.azure.cn",
    "fairfax":"https://api.{0}.us",
    "blackforest":"https://api.{0}.de",
}



    def __init__(self, conn_kv: dict, domain: str, data_source: str, **options):

        cloud = options.get("cloud") or "public"
        self._domain = domain
        if data_source.find("loganalytics") >= 0:
            self._data_source = self._CLOUD_AAD_URLS.get(cloud).format("loganalytics")
        elif data_source.find("applicationinsights") >= 0:
            self._data_source = self._CLOUD_AAD_URLS.get(cloud).format("applicationinsights")            

        self._appkey = conn_kv.get(ConnStrKeys.APPKEY)



        if self._appkey is None and conn_kv.get(ConnStrKeys.ANONYMOUS) is None:
            self._aad_helper = _MyAadHelper(ConnKeysKCSB(conn_kv, self._data_source), self._DEFAULT_CLIENTID, **options)
        else:
            self._aad_helper = None

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
        api_url = "{0}/{1}/{2}/{3}/{4}".format(self._data_source, self._API_VERSION, self._domain, id, "metadata" if is_metadata else "query")

        #
        # create Prefer header
        #

        prefer_list = []
        if self._API_VERSION != "beta":
            prefer_list.append("ai.response-thinning=false")  # returns data as kusto v1
        
        timeout = options.get("timeout")
        if timeout is not None:
            prefer_list.append("wait={0}".format(timeout))

        #
        # create headers
        #

        request_headers = {
            "x-ms-client-version": "{0}.Python.Client:{1}".format(Constants.MAGIC_CLASS_NAME, self._WEB_CLIENT_VERSION),
            "x-ms-client-request-id": "{0}.execute;{1}".format(Constants.MAGIC_CLASS_NAME, str(uuid.uuid4())),
        }
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
            raise KqlError([response.text], response)

        json_response = response.json()

        if is_metadata:
            kql_response = KqlSchemaResponse(json_response)
        else:
            kql_response = KqlQueryResponse(json_response)

        if kql_response.has_exceptions() and not accept_partial_results:
            raise KqlError(kql_response.get_exceptions(), response, kql_response)

        return kql_response

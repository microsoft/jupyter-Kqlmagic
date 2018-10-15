#-------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
#--------------------------------------------------------------------------

import uuid
import six
from datetime import timedelta, datetime
import json
import adal
import dateutil.parser
import requests
# import webbrowser
from kql.constants import Constants
from kql.kql_client import KqlResponse, KqlSchemaResponse, KqlError
from kql.my_aad_helper import _MyAadHelper, ConnKeysKCSB
 


class DraftClient(object):
    """
    """
    _DEFAULT_CLIENTID = "db662dc1-0cfe-4e1c-a843-19a68e65be58"
    _CLIENT_VERSION = "0.1.0"
    _API_VERSION = "v1"


    def __init__(self, conn_kv: dict, domain, cluster):
        """
        Loganalytics Client constructor.

        Parameters
        ----------
        """
        # print("cluster", cluster)
        # print("domain", domain)
        # print("conn_kv", conn_kv)
        self.domain = domain
        self.cluster = cluster
        self.appkey = conn_kv.get("appkey")
        if self.appkey is None:
            self._aad_helper = _MyAadHelper(ConnKeysKCSB(conn_kv, self.cluster), self._DEFAULT_CLIENTID)

    def execute(self, id, query: str, accept_partial_results=False, timeout=None):
        """ Execute a simple query or metadata query
        
        Parameters
        ----------
        id : str
            the workspaces or apps id.
        query : str
            Query to be executed
        query_endpoint : str
            The query's endpoint
        accept_partial_results : bool
            Optional parameter. If query fails, but we receive some results, we consider results as partial.
            If this is True, results are returned to client, even if there are exceptions.
            If this is False, exception is raised. Default is False.
        timeout : float, optional
            Optional parameter. Network timeout in seconds. Default is no timeout.
        """
        # https://api.applicationinsights.io/v1/apps/DEMO_APP/metadata?api_key=DEMO_KEY
        # https://api.loganalytics.io/v1/workspaces/DEMO_WORKSPACE/metadata?api_key=DEMO_KEY
        ismetadata = query.startswith('.') and query == ".show schema"
        request_payload = {"query": query}
        query_endpoint = "{0}/{1}/{2}/{3}/{4}".format(self.cluster, self._API_VERSION, self.domain, id, "metadata" if ismetadata else "query")
        # print('query_endpoint: ', query_endpoint)

        request_headers = {
            "x-ms-client-version": "{0}.Python.Client:{1}".format(Constants.MAGIC_CLASS_NAME, self._CLIENT_VERSION),
            "x-ms-client-request-id": "{0}PC.execute;{1}".format(Constants.MAGIC_CLASS_NAME[0], str(uuid.uuid4())),
        }
        if self.appkey is not None:
            request_headers["x-api-key"] = self.appkey
        else:
            request_headers["Authorization"] = self._aad_helper.acquire_token()
            # print('token: ' + request_headers["Authorization"])
        prefer_list = []
        if self._API_VERSION != "beta":
            prefer_list.append("ai.response-thinning=false") # returns data as kusto v1
        if timeout is not None:
            prefer_list.append("wait={0}".format(timeout))
        if len(prefer_list) > 0:
            request_headers["Prefer"] = ", ".join(prefer_list)
        # print('request_headers: ', request_headers)


        response = requests.get(query_endpoint, headers=request_headers) if ismetadata else requests.post(query_endpoint, headers=request_headers, json=request_payload)

        if response.status_code == 200:
            json_response = response.json()
            # print('json_response:', json_response)
            query_response = KqlSchemaResponse(json_response) if ismetadata else KqlResponse(json_response)
            if query_response.has_exceptions() and not accept_partial_results:
                raise KqlError(query_response.get_exceptions(), response, query_response)
            return query_response
        else:
            raise KqlError([response.text], response)


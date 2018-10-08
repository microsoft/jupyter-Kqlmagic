#-------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
#--------------------------------------------------------------------------

import uuid
import six
from datetime import timedelta, datetime
import re
import json
import adal
import dateutil.parser
import requests
from kql.kql_client import KqlResponse, KqlSchemaResponse, KqlError
from kql.my_aad_helper import _MyAadHelper, ConnKeysKCSB


_client_version = "0.1.0"
_api_version = "v1"
_cluster = "https://api.applicationinsights.io"

class AppinsightsClient(object):
    """
    """

    def __init__(self, conn_kv: dict):
        """
        Appinsights constructor.

        Parameters
        ----------
        """

        self.appkey = conn_kv.get("appkey")
        self._aad_helper = _MyAadHelper(ConnKeysKCSB(conn_kv, _cluster))

    def execute(self, appid, query: str, accept_partial_results=False, timeout=None):
        """ Execute a simple query
        
        Parameters
        ----------
        appid : str
            the application Id.
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
        if query.startswith('.'):
            return self._execute_get_metadata(appid)
        else:
            return self._execute_query(appid, query)

    def _execute_query(self, appid, query: str):
        """ Execute a simple query
        
        Parameters
        ----------
        query : str
            Query to be executed
        """

        query_endpoint = "{0}/{1}/apps/{2}/query".format(_cluster, _api_version, appid)
        request_payload = {"query": query}

        request_headers = {
            "Content-Type": "application/json",
            "x-ms-client-version": "ApplicationInsights.Python.Client:" + _client_version,
            "x-ms-client-request-id": "APC.execute;" + str(uuid.uuid4()),
        }

        if self.appkey is not None:
            request_headers["x-api-key"] = self.appkey
        else:
            request_headers["Authorization"] = self._aad_helper.acquire_token()

        if _api_version != "beta":
            request_headers["Prefer"] = "ai.response-thinning=false"

        response = requests.post(query_endpoint, headers=request_headers, json=request_payload)

        if response.status_code == 200:
            query_response = KqlResponse(response.json())
            if query_response.has_exceptions() and not accept_partial_results:
                raise KqlError(query_response.get_exceptions(), response, query_response)
            # print('query_response:', response.json())
            return query_response
        else:
            raise KqlError([response.text], response)

    def _execute_get_metadata(self, appid):
        """ Execute a metadata request to get query schema
        """
        # https://api.applicationinsights.io/v1/apps/DEMO_APP/metadata?api_key=DEMO_KEY
        # query_endpoint = "{0}/{1}/apps/{2}/metadata?api_key={3}".format(_cluster, _api_version, appid, self.appkey)
        query_endpoint = "{0}/{1}/apps/{2}/metadata".format(_cluster, _api_version, appid)
        request_headers = {
            "x-ms-client-version": "ApplicationInsights.Python.Client:" + _client_version,
            "x-ms-client-request-id": "APC.execute;" + str(uuid.uuid4()),
        }

        if self.appkey is not None:
            request_headers["x-api-key"] = self.appkey
        else:
            request_headers["Authorization"] = self._aad_helper.acquire_token()

        if _api_version != "beta":
            request_headers["Prefer"] = "ai.response-thinning=false"

        response = requests.get(query_endpoint, headers=request_headers)

        if response.status_code == 200:
            query_response = KqlSchemaResponse(response.json())
            # print('query_response:', response.json())
            return query_response
        else:
            raise KqlError([response.text], response)

    def _acquire_token(self):
        token_response = self.adal_context.acquire_token(self.appinsights_cluster, self.username, self.client_id)
        if token_response is not None:
            expiration_date = dateutil.parser.parse(token_response["expiresOn"])
            if expiration_date > datetime.utcnow() + timedelta(minutes=5):
                return token_response["accessToken"]

        if self.client_secret is not None and self.client_id is not None:
            token_response = self.adal_context.acquire_token_with_client_credentials(self.appinsights_cluster, self.client_id, self.client_secret)
        elif self.username is not None and self.password is not None:
            token_response = self.adal_context.acquire_token_with_username_password(
                self.appinsights_cluster, self.username, self.password, self.client_id
            )
        else:
            code = self.adal_context.acquire_user_code(self.appinsights_cluster, self.client_id)
            # print(code['message'])
            # webbrowser.open(code['verification_url'])
            token_response = self.adal_context.acquire_token_with_device_code(self.appinsights_cluster, code, self.client_id)

        return token_response["accessToken"]

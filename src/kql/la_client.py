#-------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
#--------------------------------------------------------------------------

import six
from datetime import timedelta, datetime
import re
import json
import adal
import dateutil.parser
import requests
# import webbrowser
from kql.kql_client import KqlResponse, KqlSchemaResponse, KqlError
 

_client_version = "0.1.0"

class LoganalyticsClient(object):
    """
    Kusto client wrapper for Python.

    LoganalyticsClient works with both 2.x and 3.x flavors of Python. All primitive types are supported.
    LoganalyticsClient takes care of ADAL authentication, parsing response and giving you typed result set,
    and offers familiar Python DB API.

    Test are run using nose.

    Examples
    --------
    To use LoganalyticsClient, you can choose betwen two ways of authentication.
     
    For the first option, you'll need to have your own AAD application and know your client credentials (client_id and client_secret).
    >>> kusto_cluster = 'https://help.kusto.windows.net'
    >>> kusto_client = LoganalyticsClient(kusto_cluster, client_id, client_secret='your_app_secret')

    For the second option, you can use LoganalyticsClient's client id and authenticate using your username and password.
    >>> kusto_cluster = 'https://help.kusto.windows.net'
    >>> client_id = 'e07cf1fb-c6a6-4668-b21a-f74731afa19a'
    >>> kusto_client = LoganalyticsClient(kusto_cluster, client_id, username='your_username', password='your_password')"""

    def __init__(self, workspace=None, appkey=None, version="v1"):
        """
        Kusto Client constructor.

        Parameters
        ----------
        kusto_cluster : str
            Kusto cluster endpoint. Example: https://help.kusto.windows.net
        client_id : str
            The AAD application ID of the application making the request to Kusto
        client_secret : str
            The AAD application key of the application making the request to Kusto. if this is given, then username/password should not be.
        username : str
            The username of the user making the request to Kusto. if this is given, then password must follow and the client_secret should not be given.
        password : str
            The password matching the username of the user making the request to Kusto
        version : 'v1', optional
            REST API version, defaults to v1.
        """

        self.cluster = "https://api.loganalytics.io"
        self.version = version
        self.workspace = workspace
        self.appkey = appkey

    def execute(self, workspace, query: str, accept_partial_results=False, timeout=None):
        """ Execute a simple query
        
        Parameters
        ----------
        workspace : str
            the workspace Id.
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
            return self._execute_get_metadata()
        else:
            return self._execute_query(query)

    def _execute_query(self, query: str):
        """ Execute a simple query
        
        Parameters
        ----------
        query : str
            Query to be executed
        """

        query_endpoint = "{0}/{1}/workspaces/{2}/query".format(self.cluster, self.version, self.workspace)
        request_payload = {"query": query}

        self.request_headers = {
            "Content-Type": "application/json",
            "x-api-key": self.appkey,
            "x-ms-client-version": "LogAnalytics.Python.Client:" + _client_version,
        }
        if self.version != "beta":
            prefer_str = "ai.response-thinning=false"
            self.request_headers["Prefer"] = prefer_str

        response = requests.post(query_endpoint, headers=self.request_headers, json=request_payload)

        if response.status_code == 200:
            query_response = KqlResponse(response.json())
            if query_response.has_exceptions() and not accept_partial_results:
                raise KqlError(query_response.get_exceptions(), response, query_response)
            # print('query_response:', response.json())
            return query_response
        else:
            raise KqlError([response.text], response)

    def _execute_get_metadata(self):
        """ Execute a metadata request to get query schema
        """
        # https://api.loganalytics.io/v1/workspaces/DEMO_WORKSPACE/metadata?api_key=DEMO_KEY
        query_endpoint = "{0}/{1}/workspaces/{2}/metadata?api_key={3}".format(self.cluster, self.version, self.workspace, self.appkey)
        self.request_headers = {
            "x-ms-client-version": "ApplicationInsights.Python.Client:" + _client_version,
        }
        if self.version != "beta":
            prefer_str = "ai.response-thinning=false"
            self.request_headers["Prefer"] = prefer_str
        response = requests.get(query_endpoint, headers=self.request_headers)

        if response.status_code == 200:
            query_response = KqlSchemaResponse(response.json())
            # print('query_response:', response.json())
            return query_response
        else:
            raise KqlError([response.text], response)

    def _acquire_token(self):
        token_response = self.adal_context.acquire_token(self.loganalytics_cluster, self.username, self.client_id)
        if token_response is not None:
            expiration_date = dateutil.parser.parse(token_response["expiresOn"])
            if expiration_date > datetime.utcnow() + timedelta(minutes=5):
                return token_response["accessToken"]

        if self.client_secret is not None and self.client_id is not None:
            token_response = self.adal_context.acquire_token_with_client_credentials(self.loganalytics_cluster, self.client_id, self.client_secret)
        elif self.username is not None and self.password is not None:
            token_response = self.adal_context.acquire_token_with_username_password(
                self.loganalytics_cluster, self.username, self.password, self.client_id
            )
        else:
            code = self.adal_context.acquire_user_code(self.loganalytics_cluster, self.client_id)
            # print(code['message'])
            # webbrowser.open(code['verification_url'])
            token_response = self.adal_context.acquire_token_with_device_code(self.loganalytics_cluster, code, self.client_id)

        return token_response["accessToken"]

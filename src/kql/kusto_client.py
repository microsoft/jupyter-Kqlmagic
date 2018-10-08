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
from azure.kusto.data.request import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.exceptions import KustoServiceError

# from azure.kusto.data import KustoClient
from azure.kusto.data._response import WellKnownDataSet
from kql.my_aad_helper import _MyAadHelper
from kql.kql_client import KqlResponse, KqlError




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

    def __init__(self, conn_kv):
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
        kusto_cluster = "https://{0}.kusto.windows.net".format(conn_kv["cluster"])

        if all([conn_kv.get("username"), conn_kv.get("password")]):
            kcsb = KustoConnectionStringBuilder.with_aad_user_password_authentication(kusto_cluster, conn_kv.get("username"), conn_kv.get("password"))
        elif all([conn_kv.get("clientid"), conn_kv.get("clientsecret")]):
            kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(kusto_cluster, conn_kv.get("clientid"), conn_kv.get("clientsecret"))
        elif all([conn_kv.get("clientid"), conn_kv.get("certificate"), conn_kv.get("certificate_thumbprint")]):
            kcsb = KustoConnectionStringBuilder.with_aad_application_certificate_authentication(
                kusto_cluster, conn_kv.get("clientid"), conn_kv.get("certificate"), conn_kv.get("certificate_thumbprint")
            )
        else:
            kcsb = KustoConnectionStringBuilder.with_aad_device_authentication(kusto_cluster)

        if conn_kv.get("tenant") is not None:
            kcsb.authority_id = conn_kv.get("tenant")

        self.client = KustoClient(kcsb)

        # replace aadhelper to use remote browser in interactive mode
        self.client._aad_helper = _MyAadHelper(kcsb)

        self.mgmt_endpoint_version = "v2" if self.client._mgmt_endpoint.endswith("v2/rest/query") else "v1"
        self.query_endpoint_version = "v2" if self.client._query_endpoint.endswith("v2/rest/query") else "v1"

    def execute(self, kusto_database, query, accept_partial_results=False, timeout=None):
        """ Execute a simple query or management command

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
        timeout : float, optional
            Optional parameter. Network timeout in seconds. Default is no timeout.
        """
        endpoint_version = self.mgmt_endpoint_version if query.startswith(".") else self.query_endpoint_version
        get_raw_response=True
        response = self.client.execute(kusto_database, query, accept_partial_results, timeout, get_raw_response)
        return KqlResponse(response, endpoint_version)

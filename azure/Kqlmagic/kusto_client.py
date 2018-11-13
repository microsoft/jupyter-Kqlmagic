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

from Kqlmagic.my_aad_helper import _MyAadHelper, ConnKeysKCSB
from Kqlmagic.kql_client import KqlQueryResponse, KqlError
from Kqlmagic.constants import Constants, ConnStrKeys
from Kqlmagic.version import VERSION


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
    _DATA_SOURCE_TEMPLATE = "https://{0}.kusto.windows.net"

    _WEB_CLIENT_VERSION = VERSION

    def __init__(self, conn_kv:dict):
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

        cluster_name = conn_kv[ConnStrKeys.CLUSTER]
        data_source = cluster_name if cluster_name.find("://") >= 0 else self._DATA_SOURCE_TEMPLATE.format(cluster_name)

        self._mgmt_endpoint = self._MGMT_ENDPOINT_TEMPLATE.format(data_source, self._MGMT_ENDPOINT_VERSION)
        self._query_endpoint = self._QUERY_ENDPOINT_TEMPLATE.format(data_source, self._QUERY_ENDPOINT_VERSION)
        self._aad_helper = _MyAadHelper(ConnKeysKCSB(conn_kv, data_source), self._DEFAULT_CLIENTID)

    def execute(self, kusto_database, kusto_query, accept_partial_results=False, **options):
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
        options["timeout"] : float, optional
            Optional parameter. Network timeout in seconds. Default is no timeout.
        """
        if kusto_query.startswith("."):
            endpoint_version = self._MGMT_ENDPOINT_VERSION
            endpoint = self._mgmt_endpoint  
        else:
            endpoint_version = self._QUERY_ENDPOINT_VERSION
            endpoint = self._query_endpoint

        request_payload = {
            "db": kusto_database, 
            "csl": kusto_query,
        }

        access_token = self._aad_helper.acquire_token()
        request_headers = {
            "Authorization": access_token,
            "Accept": "application/json",
            "Accept-Encoding": "gzip,deflate",
            "Content-Type": "application/json; charset=utf-8",
            "Fed": "True",
            "x-ms-client-version": "{0}.Python.Client:{1}".format(Constants.MAGIC_CLASS_NAME, self._WEB_CLIENT_VERSION),
            "x-ms-client-request-id": "{0}.execute;{1}".format(Constants.MAGIC_CLASS_NAME, str(uuid.uuid4())),
        }

        response = requests.post(endpoint, headers=request_headers, json=request_payload, timeout=options.get("timeout"))

        if response.status_code != requests.codes.ok:  # pylint: disable=E1101
            raise KqlError([response.text], response)

        kql_response = KqlQueryResponse(response.json(), endpoint_version)

        if kql_response.has_exceptions() and not accept_partial_results:
            raise KqlError(kql_response.get_exceptions(), response, kql_response)

        return kql_response


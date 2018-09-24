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

# Regex for TimeSpan
_TIMESPAN_PATTERN = re.compile(r"(-?)((?P<d>[0-9]*).)?(?P<h>[0-9]{2}):(?P<m>[0-9]{2}):(?P<s>[0-9]{2}(\.[0-9]+)?$)")



class KqlResult(dict):
    """ Simple wrapper around dictionary, to enable both index and key access to rows in result """

    def __init__(self, index2column_mapping, *args, **kwargs):
        super(KqlResult, self).__init__(*args, **kwargs)
        # TODO: this is not optimal, if client will not access all fields.
        # In that case, we are having unnecessary perf hit to convert Timestamp, even if client don't use it.
        # In this case, it would be better for KqlResult to extend list class. In this case,
        # KqlResultIter.index2column_mapping should be reversed, e.g. column2index_mapping.
        self.index2column_mapping = index2column_mapping

    def __getitem__(self, key):
        if isinstance(key, slice):
            start = min(key.start or 0, len(self))
            end = min(key.stop or len(self), len(self))
            mapping = self.index2column_mapping[start:end]
            dic = dict([(c, dict.__getitem__(self, c)) for c in mapping ])
            return KqlResult(mapping, dic)
        elif isinstance(key, six.integer_types):
            val = dict.__getitem__(self, self.index2column_mapping[key])
        else:
            val = dict.__getitem__(self, key)
        return val


class KqlResponseTable(six.Iterator):
    """ Iterator over returned rows """

    def __init__(self, response_table):
        self.rows = response_table["Rows"]
        self.columns = response_table["Columns"]
        self.index2column_mapping = []
        self.index2type_mapping = []
        for c in self.columns:
            self.index2column_mapping.append(c["ColumnName"])
            ctype = c["ColumnType"] if "ColumnType" in c else c["DataType"]
            self.index2type_mapping.append(ctype)
        self.row_index = 0
        self._rows_count = len(self.rows)
        # Here we keep converter functions for each type that we need to take special care (e.g. convert)
        self.converters_lambda_mappings = {
            "datetime": self.to_datetime,
            "timespan": self.to_timedelta,
            "DateTime": self.to_datetime,
            "TimeSpan": self.to_timedelta,
            "dynamic": self.to_object,
        }

    @staticmethod
    def to_object(value):
        if value is None:
            return None
        return json.loads(value)

    @staticmethod
    def to_datetime(value):
        if value is None:
            return None
        return dateutil.parser.parse(value)

    @staticmethod
    def to_timedelta(value):
        """Converts a string to a timedelta."""
        if value is None:
            return None
        if isinstance(value, (six.integer_types, float)):
            return timedelta(microseconds=(float(value) / 10))
        match = _TIMESPAN_PATTERN.match(value)
        if match:
            if match.group(1) == "-":
                factor = -1
            else:
                factor = 1
            return factor * timedelta(
                days=int(match.group("d") or 0),
                hours=int(match.group("h")),
                minutes=int(match.group("m")),
                seconds=float(match.group("s")),
            )
        else:
            raise ValueError("Timespan value '{}' cannot be decoded".format(value))

    def __iter__(self):
        self.row_index = 0
        return self

    def __next__(self):
        if self.row_index >= self.rows_count:
            raise StopIteration
        row = self.rows[self.row_index]
        result_dict = {}
        for index, value in enumerate(row):
            data_type = self.index2type_mapping[index]
            if data_type in self.converters_lambda_mappings:
                result_dict[self.index2column_mapping[index]] = self.converters_lambda_mappings[data_type](value)
            else:
                result_dict[self.index2column_mapping[index]] = value
        self.row_index = self.row_index + 1
        return KqlResult(self.index2column_mapping, result_dict)

    @property
    def columns_name(self):
        return self.index2column_mapping

    @property
    def columns_type(self):
        return self.index2type_mapping

    @property
    def rows_count(self):
        return self._rows_count

    @property
    def columns_count(self):
        return len(self.columns)

    def fetchall(self):
        """ Returns iterator to get rows from response """
        return self.__iter__()

    def iter_all(self):
        """ Returns iterator to get rows from response """
        return self.__iter__()


class KqlResponse(object):
    """ Wrapper for response """

    # TODO: add support to get additional infromation from response, like execution time

    def __init__(self, json_response, endpoint_version="v1"):
        self.json_response = json_response
        self.endpoint_version = endpoint_version
        if self.endpoint_version == "v2":
            self.all_tables = [t for t in json_response if t["FrameType"] == "DataTable"]
            self.tables = [t for t in json_response if t["FrameType"] == "DataTable" and t["TableKind"] == "PrimaryResult"]
        else:
            self.all_tables = self.json_response["Tables"]
            tables_num = self.json_response["Tables"].__len__()
            last_table = self.json_response["Tables"][tables_num - 1]
            self.tables = [self.json_response["Tables"][r[0]] for r in last_table["Rows"] if r[2] == "GenericResult" or r[2] == "PrimaryResult"]
            if len(self.tables) == 0:
                self.tables = self.all_tables[:1]
        self.primary_results = [KqlResponseTable(t) for t in self.tables]


    def _get_endpoint_version(self, json_response):
        try:
            tables_num = json_response["Tables"].__len__()
            return "v1"
        except:
            return "v2"

    @property
    def visualization_results(self):
        if self.endpoint_version == "v2":
            for table in self.all_tables:
                if table["TableName"] == "@ExtendedProperties":
                    for row in table["Rows"]:
                        if row[1] == "Visualization":
                            # print('visualization_properties: {}'.format(row[2]))
                            return json.loads(row[2])
        else:
            tables_num = self.json_response["Tables"].__len__()
            last_table = self.json_response["Tables"][tables_num - 1]
            for row in last_table["Rows"]:
                if row[2] == "@ExtendedProperties":
                    table = self.json_response["Tables"][row[0]]
                    # print('visualization_properties: {}'.format(table['Rows'][0][0]))
                    return json.loads(table["Rows"][0][0])
        return {}

    @property
    def completion_query_info_results(self):
        if self.endpoint_version == "v2":
            for table in self.all_tables:
                if table["TableName"] == "QueryCompletionInformation":
                    cols_idx_map = self._map_columns_to_index(table["Columns"])
                    event_type_name_idx = cols_idx_map.get("EventTypeName")
                    payload_idx = cols_idx_map.get("Payload")
                    if event_type_name_idx is not None and payload_idx is not None:
                        for row in table["Rows"]:
                            if row[event_type_name_idx] == "QueryInfo":
                                return json.loads(row[payload_idx])
        else:
            tables_num = self.json_response["Tables"].__len__()
            last_table = self.json_response["Tables"][tables_num - 1]
            for r in last_table["Rows"]:
                if r[2] == "QueryStatus":
                    t = self.json_response["Tables"][r[0]]
                    for sr in t["Rows"]:
                        if sr[2] == "Info":
                            info = {"StatusCode": sr[3], "StatusDescription": sr[4], "Count": sr[5]}
                            # print('Info: {}'.format(info))
                            return info
        return {}

    @property
    def completion_query_resource_consumption_results(self):
        if self.endpoint_version == "v2":
            for table in self.all_tables:
                if table["TableName"] == "QueryCompletionInformation":
                    cols_idx_map = self._map_columns_to_index(table["Columns"])
                    event_type_name_idx = cols_idx_map.get("EventTypeName")
                    payload_idx = cols_idx_map.get("Payload")
                    if event_type_name_idx is not None and payload_idx is not None:
                        for row in table["Rows"]:
                            if row[event_type_name_idx] == "QueryResourceConsumption":
                                return json.loads(row[payload_idx])
        else:
            tables_num = self.json_response["Tables"].__len__()
            last_table = self.json_response["Tables"][tables_num - 1]
            for r in last_table["Rows"]:
                if r[2] == "QueryStatus":
                    t = self.json_response["Tables"][r[0]]
                    for sr in t["Rows"]:
                        if sr[2] == "Stats":
                            stats = sr[4]
                            # print('stats: {}'.format(stats))
                            return json.loads(stats)
        return {}

    def _map_columns_to_index(self, columns: list):
        map = {}
        for idx, col in enumerate(columns):
            map[col["ColumnName"]] = idx
        return map

    def get_raw_response(self):
        return self.json_response

    def get_table_count(self):
        return len(self.tables)

    def has_exceptions(self):
        return "Exceptions" in self.json_response

    def get_exceptions(self):
        return self.json_response["Exceptions"]


# used in Kqlmagic
class KqlError(Exception):
    """
    Represents error returned from server. Error can contain partial results of the executed query.
    """

    def __init__(self, messages, http_response, kql_response=None):
        super(KqlError, self).__init__(messages)
        self.http_response = http_response
        self.kql_response = kql_response

    def get_raw_http_response(self):
        return self.http_response

    def is_semantic_error(self):
        return self.http_response.text.startswith("Semantic error:")

    def has_partial_results(self):
        return self.kql_response is not None

    def get_partial_results(self):
        return self.kql_response




class Kql_Client(object):
    """
    Kql client wrapper for Python.

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

    def __init__(
        self,
        kusto_cluster,
        client_id=None,
        client_secret=None,
        username=None,
        password=None,
        certificate=None,
        certificate_thumbprint=None,
        authority=None,
    ):
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
        if all([username, password]):
            kcsb = KustoConnectionStringBuilder.with_aad_user_password_authentication(kusto_cluster, username, password)
        elif all([client_id, client_secret]):
            kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(kusto_cluster, client_id, client_secret)
        elif all([client_id, certificate, certificate_thumbprint]):
            kcsb = KustoConnectionStringBuilder.with_aad_application_certificate_authentication(
                kusto_cluster, client_id, certificate, certificate_thumbprint
            )
        else:
            kcsb = KustoConnectionStringBuilder.with_aad_device_authentication(kusto_cluster)

        if authority:
            kcsb.authority_id = authority

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

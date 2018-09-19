import six
from datetime import timedelta, datetime
import re
import json
import adal
import dateutil.parser
import requests

# Regex for TimeSpan
TIMESPAN_PATTERN = re.compile(r"((?P<d>[0-9]*).)?(?P<h>[0-9]{2}):(?P<m>[0-9]{2}):(?P<s>[0-9]{2})(.(?P<ms>[0-9]*))?")

__version__ = "0.1.0"


class AppinsightsResult(dict):
    """ Simple wrapper around dictionary, to enable both index and key access to rows in result """

    def __init__(self, index2column_mapping, *args, **kwargs):
        super(AppinsightsResult, self).__init__(*args, **kwargs)
        # TODO: this is not optimal, if client will not access all fields.
        # In that case, we are having unnecessary perf hit to convert Timestamp, even if client don't use it.
        # In this case, it would be better for KustoResult to extend list class. In this case,
        # KustoResultIter.index2column_mapping should be reversed, e.g. column2index_mapping.
        self.index2column_mapping = index2column_mapping

    def __getitem__(self, key):
        if isinstance(key, slice):
            start = min(key.start or 0, len(self))
            end = min(key.stop or len(self), len(self))
            mapping = self.index2column_mapping[start:end]
            dic = dict([(c, dict.__getitem__(self, c)) for c in mapping ])
            return AppinsightsResult(mapping, dic)
        elif isinstance(key, six.integer_types):
            val = dict.__getitem__(self, self.index2column_mapping[key])
        else:
            val = dict.__getitem__(self, key)
        return val



class AppinsightsResponseTable(six.Iterator):
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
        if value is None:
            return None
        if isinstance(value, (six.integer_types, float)): 
            return timedelta(microseconds=(float(value) / 10))         
        m = TIMESPAN_PATTERN.match(value)
        if m:
            if match.group(1) == "-": 
                factor = -1 
            else: 
                factor = 1 
            return factor * timedelta(
                days=int(m.group("d") or 0),
                hours=int(m.group("h")),
                minutes=int(m.group("m")),
                seconds=int(m.group("s")),
                milliseconds=int(m.group("ms") or 0),
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
        return AppinsightsResult(self.index2column_mapping, result_dict)

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


class AppinsightsResponse(object):
    """ Wrapper for response """

    # TODO: add support to get additional infromation from response, like execution time

    def __init__(self, json_response):
        self.json_response = json_response
        self.primary_results = []
        tables_num = self.json_response["Tables"].__len__()
        last_table = self.json_response["Tables"][tables_num - 1]
        for r in last_table["Rows"]:
            if r[2] == "GenericResult" or r[2] == "PrimaryResult":
                t = self.json_response["Tables"][r[0]]
                self.primary_results.append(AppinsightsResponseTable(t))
        if len(self.primary_results) == 0:
            t = self.json_response["Tables"][0]
            self.primary_results.append(AppinsightsResponseTable(t))

    @property
    def visualization_results(self):
        tables_num = self.json_response["Tables"].__len__()
        last_table = self.json_response["Tables"][tables_num - 1]
        for r in last_table["Rows"]:
            if r[2] == "@ExtendedProperties":
                t = self.json_response["Tables"][r[0]]
                # print('visualization_properties: {}'.format(t['Rows'][0][0]))
                return json.loads(t["Rows"][0][0])
        return None

    @property
    def completion_query_info_results(self):
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

    def get_raw_response(self):
        return self.json_response

    def get_table_count(self):
        return len(self.json_response["Tables"])

    def has_exceptions(self):
        return "Exceptions" in self.json_response

    def get_exceptions(self):
        return self.json_response["Exceptions"]


# used in Kqlmagic
class AppinsightsError(Exception):
    """
    Represents error returned from server. Error can contain partial results of the executed query.
    """

    def __init__(self, messages, http_response, appinsights_response=None):
        super(AppinsightsError, self).__init__(messages)
        self.http_response = http_response
        self.appinsights_response = appinsights_response

    def get_raw_http_response(self):
        return self.http_response

    def is_semantic_error(self):
        return self.http_response.text.startswith("Semantic error:")

    def has_partial_results(self):
        return self.appinsights_response is not None

    def get_partial_results(self):
        return self.appinsights_response


class AppinsightsClient(object):
    """
    Kusto client wrapper for Python.

    AppinsightsClient works with both 2.x and 3.x flavors of Python. All primitive types are supported.
    AppinsightsClient takes care of ADAL authentication, parsing response and giving you typed result set,
    and offers familiar Python DB API.

    Test are run using nose.

    Examples
    --------
    To use AppinsightsClient, you can choose betwen two ways of authentication.
     
    For the first option, you'll need to have your own AAD application and know your client credentials (client_id and client_secret).
    >>> kusto_cluster = 'https://help.kusto.windows.net'
    >>> kusto_client = AppinsightsClient(kusto_cluster, client_id, client_secret='your_app_secret')

    For the second option, you can use AppinsightsClient's client id and authenticate using your username and password.
    >>> kusto_cluster = 'https://help.kusto.windows.net'
    >>> client_id = 'e07cf1fb-c6a6-4668-b21a-f74731afa19a'
    >>> kusto_client = AppinsightsClient(kusto_cluster, client_id, username='your_username', password='your_password')

    After connecting, use the kusto_client instance to execute a management command or a query: 
    >>> kusto_database = 'Samples'
    >>> response = kusto_client.execute_query(kusto_database, 'StormEvents | take 10')
    You can access rows now by index or by key.
    >>> for row in response.iter_all():
    >>>    print(row[0])
    >>>    print(row["ColumnName"])    """

    def __init__(self, appid=None, appkey=None, version="v1"):
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

        self.cluster = "https://api.applicationinsights.io"
        self.version = version
        self.appid = appid
        self.appkey = appkey

    def execute(self, appid, query: str, accept_partial_results=False, timeout=None):
        """ Execute a simple query
        
        Parameters
        ----------
        kusto_database : str
            Database against query will be executed.
        kusto_query : str
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
        return self.execute_query(appid, query, accept_partial_results, timeout)

    def execute_query(self, appid, query: str, accept_partial_results=False, timeout=None):
        """ Execute a simple query 
        
        Parameters
        ----------
        kusto_database : str
            Database against query will be executed.
        kusto_query : str
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
        query_endpoint = "{0}/{1}/apps/{2}/query".format(self.cluster, self.version, self.appid)
        return self._execute(query, query_endpoint, accept_partial_results, timeout)

    def _execute(self, query, query_endpoint, accept_partial_results=False, timeout=None):
        """ Executes given query against this client """

        request_payload = {"query": query}

        self.request_headers = {
            "Content-Type": "application/json",
            "x-api-key": self.appkey,
            "x-ms-client-version": "ApplicationInsights.Python.Client:" + __version__,
        }
        if self.version != "beta":
            prefer_str = "ai.response-thinning=false"
            self.request_headers["Prefer"] = prefer_str

        response = requests.post(query_endpoint, headers=self.request_headers, json=request_payload)

        if response.status_code == 200:
            appinsights_response = AppinsightsResponse(response.json())
            if appinsights_response.has_exceptions() and not accept_partial_results:
                raise AppinsightsError(appinsights_response.get_exceptions(), response, appinsights_response)
            # print('appinsights_response:', response.json())
            return appinsights_response
        else:
            raise AppinsightsError([response.text], response)

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

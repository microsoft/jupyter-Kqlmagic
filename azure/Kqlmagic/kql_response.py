# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

from datetime import timedelta, datetime
import re
import json
from .log import logger
import ijson

from .Kql_response_wrapper import CSV_table_reader


import adal
import dateutil.parser
import six
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
            dic = dict([(c, dict.__getitem__(self, c)) for c in mapping])
            return KqlResult(mapping, dic)
        elif isinstance(key, six.integer_types):
            val = dict.__getitem__(self, self.index2column_mapping[key])
        else:
            val = dict.__getitem__(self, key)
        return val


class KqlResponseTable(six.Iterator):
    """ Iterator over returned rows """

    def __init__(self, id, response_table):
        self.id = id
        self.rows = CSV_table_reader(response_table["Rows"]) if isinstance(response_table["Rows"], str) else response_table["Rows"]
        self.columns = response_table["Columns"]
        self.index2column_mapping = []
        self.index2type_mapping = []
        for c in self.columns:
            self.index2column_mapping.append(c["ColumnName"])
            ctype = c["ColumnType"] if "ColumnType" in c else c["DataType"]
            self.index2type_mapping.append(ctype)
        self.row_index = 0
        self._rows_count = len(self.rows) #sum([1 for r in self.rows if isinstance(r,list)])
        # Here we keep converter functions for each type that we need to take special care (e.g. convert)

        # index MUST be lowercase !!!
        self.converters_lambda_mappings = {
            "datetime": self.to_datetime,
            "timespan": self.to_timedelta,
            "dynamic": self.to_object,
        }


    @staticmethod
    def to_object(value):
        try:
            return json.loads(value) if value and isinstance(value, str) else value if value else None
        except Exception:
            return value


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
                days=int(match.group("d") or 0), hours=int(match.group("h")), minutes=int(match.group("m")), seconds=float(match.group("s"))
            )
        else:
            raise ValueError(f"Timespan value '{value}' cannot be decoded")


    def __iter__(self):
        self.row_index = 0
        return self


    def __next__(self):
        if self.row_index >= self.rows_count:
            raise StopIteration
        row = self.rows[self.row_index]
        result_dict = {}
        for index, value in enumerate(row):
            data_type = self.index2type_mapping[index].lower()
            column_name = self.index2column_mapping[index]
            if data_type in self.converters_lambda_mappings:
                value = self.converters_lambda_mappings[data_type](value)
            elif self.rows_count == 1 and self.columns_count == 1 and column_name == "DatabaseSchema" and data_type == "string":
                value = self.to_object(value)
            result_dict[column_name] = value
        self.row_index = self.row_index + 1
        return KqlResult(self.index2column_mapping, result_dict)


    @property
    def columns_name(self):
        return self.index2column_mapping


    @property
    def is_partial(self):
        return len(self.rows) > self._rows_count


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


class KqlSchemaResponse(object):

    def __init__(self, json_response):
        self.json_response = json_response
        self.table = json_response["tables"]


    def has_exceptions(self):
        return "Exceptions" in self.json_response


    def get_exceptions(self):
        return self.json_response["Exceptions"]


class KqlQueryResponse(object):
    """ Wrapper for response """

    # TODO: add support to get additional infromation from response, like execution time

    def __init__(self, json_response, endpoint_version="v1"):
        self.json_response = json_response
        self.endpoint_version = "v1" if not isinstance(self.json_response, list)  else endpoint_version
        self.visualization = None

        if self.endpoint_version == "v2":
            self.all_tables = [t for t in json_response if t["FrameType"] == "DataTable"]
            self.tables = [t for t in json_response if t["FrameType"] == "DataTable" and t["TableKind"] == "PrimaryResult"]
            self.primary_results = [KqlResponseTable(t["TableId"], t) for t in self.tables]
            self.dataSetCompletion = [f for f in json_response if f["FrameType"] == "DataSetCompletion"]
        else:
            self.all_tables = self.json_response["Tables"]
            tables_num = self.all_tables.__len__()
            last_table = self.all_tables[tables_num - 1]
            if tables_num < 2:
                self.tables = []
            else:
                rows_last_table = CSV_table_reader(last_table["Rows"]) if isinstance(last_table["Rows"], str) else last_table["Rows"]
                self.tables = [self.all_tables[r[0]] for r in rows_last_table if r[2] == "GenericResult" or r[2] == "PrimaryResult"]
            if len(self.tables) == 0:
                self.tables = self.all_tables[:1]
            self.primary_results = [KqlResponseTable(idx, t) for idx, t in enumerate(self.tables)]
            self.dataSetCompletion = []
 

    def _get_endpoint_version(self, json_response):
        try:
            tables_num = json_response["Tables"].__len__()  # pylint: disable=W0612
            return "v1"
        except:
            return "v2"


    @property
    def visualization_results(self):
        if self.visualization is None:
            self.visualization = {}
            if self.endpoint_version == "v2":
                for table in self.all_tables:
                    if table["TableName"] == "@ExtendedProperties" and table["TableKind"] == "QueryProperties":
                        cols_idx_map = self._map_columns_to_index(table["Columns"])
                        types = self._get_columns_types(table["Columns"])
                        key_idx = cols_idx_map.get("Key")
                        id_idx = cols_idx_map.get("TableId")
                        value_idx = cols_idx_map.get("Value")
                        if (
                            key_idx is not None
                            and id_idx is not None
                            and value_idx is not None
                            and types[key_idx] == "string"
                            and types[id_idx] == "int"
                            and types[value_idx] == "dynamic"
                        ):
                            for row in table["Rows"]:
                                if row[key_idx] == "Visualization":
                                    # print(f'visualization raw properties for table {id_idx}: {row[value_idx]}')
                                    value = row[value_idx]
                                    self.visualization[row[id_idx]] = self._dynamic_to_object(value)
            else:
                tables_num = self.all_tables.__len__()
                if tables_num > 1:
                    last_table = self.json_response["Tables"][tables_num - 1]
                    for row in last_table["Rows"]:
                        if row[2] == "@ExtendedProperties" and row[1] == "QueryProperties":
                            table = self.json_response["Tables"][row[0]]
                            # print(f'visualization raw properties for first table: {table['Rows'][0][0]}')
                            value = table["Rows"][0][0]
                            self.visualization[0] = self._dynamic_to_object(value)
        return self.visualization


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
                                value = row[payload_idx]
                                return self._dynamic_to_object(value)
        else:
            tables_num = self.all_tables.__len__()
            if tables_num > 1:
                last_table = self.all_tables[tables_num - 1]
                for r in last_table["Rows"]:
                    if r[2] == "QueryStatus":
                        t = self.json_response["Tables"][r[0]]
                        for sr in t["Rows"]:
                            if sr[2] == "Info":
                                info = {"StatusCode": sr[3], "StatusDescription": sr[4], "Count": sr[5]}
                                # print(f'Info: {info}')
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
                                value = row[payload_idx]
                                return self._dynamic_to_object(value)
        else:
            tables_num = self.all_tables.__len__()
            if tables_num > 1:
                last_table = self.all_tables[tables_num - 1]
                for r in last_table["Rows"]:
                    if r[2] == "QueryStatus":
                        t = self.json_response["Tables"][r[0]]
                        for sr in t["Rows"]:
                            if sr[2] == "Stats":
                                stats = sr[4]
                                # print(f'stats: {stats}')
                                return self._dynamic_to_object(stats)
        return {}


    @property
    def dataSetCompletion_results(self):
        return self.dataSetCompletion


    def _map_columns_to_index(self, columns: list):
        map = {}
        for idx, col in enumerate(columns):
            map[col["ColumnName"]] = idx
        return map


    def _get_columns_types(self, columns: list):
        map = []
        for col in columns:
            map.append(col["ColumnType"])
        return map


    def get_raw_response(self):
        return self.json_response


    def get_table_count(self):
        return len(self.tables)


    def has_exceptions(self):
        return "Exceptions" in self.json_response


    def get_exceptions(self):
        return self.json_response["Exceptions"]


    @staticmethod
    def _dynamic_to_object(value):
        try:
            return json.loads(value) if value and isinstance(value, str) else value if value else None
        except Exception:
            return value


class KqlError(Exception):
    """
    Represents error returned from server. Error can contain partial results of the executed query.
    """

    def __init__(self, message, http_response, kql_response=None):
        super(KqlError, self).__init__(message)
        self.message = message
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


# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

import json
from datetime import datetime
import collections


import dateutil.parser


from .dependencies import Dependencies


class KqlRow(collections.Iterator):

    def __init__(self, row, col_num, **options):
        self.options = options
        self.row = row
        self.column_index = 0
        self.columns_count = col_num


    def __iter__(self):
        self.column_index = 0
        return self


    def __next__(self):
        if self.column_index >= self.columns_count:
            raise StopIteration
        val = self.__getitem__(self.column_index)
        self.column_index = self.column_index + 1
        return val


    def __getitem__(self, key):
        if isinstance(key, slice):
            s = self.row[key]
            return KqlRow(s, len(s), **self.options)
        else:
            return self.row[key]


    def __len__(self):
        return self.columns_count


    def __eq__(self, other):
        if len(other) != self.columns_count:
            return False
        for i in range(self.columns_count):
            s = self.__getitem__(i)
            o = other[i]
            if o != s:
                return False
        return True


    def __str__(self):
        return ", ".join(str(self.__getitem__(i)) for i in range(self.columns_count))


    def __repr__(self):
        return self.row.__repr__()


class KqlRowsIter(collections.Iterator):
    """ Iterator over returned rows, limited by size """

    def __init__(self, table, row_num, col_num, **options):
        self.options = options
        self.table = table
        self.row_index = 0
        self.rows_count = row_num
        self.col_num = col_num


    def __iter__(self):
        self.row_index = 0
        self.iter_all_iter = self.table.iter_all()
        return self


    def __next__(self):
        if self.row_index >= self.rows_count:
            raise StopIteration
        self.row_index = self.row_index + 1
        return KqlRow(self.iter_all_iter.__next__(), self.col_num, **self.options)


    def __len__(self):
        return self.rows_count


class KqlResponse(object):

    # Object constructor
    def __init__(self, response, **options):
        self.json_response = response.json_response
        self.options = options
        self.completion_query_info = response.completion_query_info_results
        self.completion_query_resource_consumption = response.completion_query_resource_consumption_results
        self.dataSetCompletion = response.dataSetCompletion_results
        self.tables = [
            KqlTableResponse(
                t, 
                response.extended_properties.get(t.id, {}),
                **options) 
            for t in response.primary_results]


class KqlTableResponse(object):

    def __init__(self, data_table, extended_properties:dict, **options):
        self.options = options
        self._extended_properties = extended_properties
        self.data_table = data_table
        self.columns_count = self.data_table.columns_count


    def fetchall(self):
        return KqlRowsIter(self.data_table, self.data_table.rows_count, self.data_table.columns_count, **self.options)


    def fetchmany(self, size):
        return KqlRowsIter(self.data_table, min(size, self.data_table.rows_count), self.data_table.columns_count, **self.options)


    def rowcount(self):
        return self.data_table.rows_count


    def colcount(self):
        return self.data_table.columns_count


    def recordscount(self):
        return self.data_table.rows_count


    def ispartial(self):
        return self.data_table.is_partial


    def keys(self):
        return self.data_table.columns_name


    def types(self):
        return self.data_table.columns_type
    

    @property
    def extended_properties(self):
        " returns properties as specified in ExtendedProperties table"
        return self._extended_properties


    @property
    def datafarme_types(self):
        return [self.KQL_TO_DATAFRAME_DATA_TYPES.get(t) for t in self.data_table.columns_type]


    def _map_columns_to_index(self, columns: list):
        map = {}
        for idx, col in enumerate(columns):
            map[col["ColumnName"]] = idx
        return map


    def returns_rows(self):
        return self.data_table.rows_count > 0


    def to_dataframe(self, raise_errors=True, options=None):
        """Returns Pandas data frame."""
        
        options = options or {}
        pandas = Dependencies.get_module("pandas")

        if self.data_table.columns_count == 0 or self.data_table.rows_count == 0:
            # return pandas.DataFrame()
            pass

        frame = pandas.DataFrame(self.data_table.rows, columns=self.data_table.columns_name)

        for (idx, col_name) in enumerate(self.data_table.columns_name):
            col_type = self.data_table.columns_type[idx].lower()

            if col_type == "timespan":
                frame[col_name] = pandas.to_timedelta(
                    frame[col_name].apply(lambda t: t.replace(".", " days ") if type(t) is str and "." in t.split(":")[0] else t)
                )

            elif col_type == "datetime":
                frame[col_name] = pandas.to_datetime(
                    frame[col_name].apply(lambda d: d if self._is_valid_datetime(d) else None)
                )

            elif col_type == "string":
                # frame[col_name] = frame[col_name].apply(lambda x: json_dumps(x) if type(x) == str else x)
                pass

            elif col_type == "dynamic":
                if options.get("dynamic_to_dataframe") == "str":
                    frame[col_name] = frame[col_name].apply(lambda x: self._dynamic_to_str(x))
                else:
                    frame[col_name] = frame[col_name].apply(lambda x: self._dynamic_to_object(x))

            elif col_type in self.KQL_TO_DATAFRAME_DATA_TYPES:
                pandas_type = self.KQL_TO_DATAFRAME_DATA_TYPES[col_type]
                # NA type promotion
                if pandas_type == "int64" or pandas_type == "int32":
                    for i in range(0, len(frame[col_name])):
                        if frame[col_name][i] is None or str(frame[col_name][i]) == "nan":
                            pandas_type = "float64"
                            break
                elif pandas_type == "bool":
                    for i in range(0, len(frame[col_name])):
                        if frame[col_name][i] is None or str(frame[col_name][i]) == "nan":
                            pandas_type = "object"
                            break
                frame[col_name] = frame[col_name].astype(pandas_type, errors="raise" if raise_errors else "ignore")
        return frame


    def _is_valid_datetime(self, d:str)->bool:
        # max diff in seconds from 9223372036 from 1970-01-01T00:00:00Z
        MAX_DIFF_FROM_EPOCH_IN_SECS = 9223372036  # 2**63/1000000000
        START_EPOCH_DATETIME = dateutil.parser.isoparse('1970-01-01T00:00:00Z')
        try:
            d = dateutil.parser.isoparse(d) if type(d) == str else d
            return isinstance(d, datetime) and abs((d - START_EPOCH_DATETIME).total_seconds()) < MAX_DIFF_FROM_EPOCH_IN_SECS

        except:
            return False


    @staticmethod
    def _dynamic_to_object(value):
        try:
            return json.loads(value) if value and isinstance(value, str) else value
        except Exception:
            return value


    @staticmethod
    def _dynamic_to_str(value):
        try:
            return value if isinstance(value, str) else f"{value}" if value is not None else None
        except Exception:
            return value


    # index MUST be lowercase 
    KQL_TO_DATAFRAME_DATA_TYPES = {
        "bool": "bool",
        "uint8": "int64",
        "int16": "int64",
        "uint16": "int64",
        "int": "int64",
        "uint": "int64",
        "long": "int64",
        "ulong": "int64",
        "float": "float64",
        "real": "float64",
        "decimal": "float64",
        "string": "object",
        "datetime": "datetime64[ns]",
        "guid": "object",
        "timespan": "timedelta64[ns]",
        "dynamic": "object",
        # Support V1
        # "datetime": "datetime64[ns]",
        "int32": "int32",
        "int64": "int64",
        "double": "float64",
        # "string": "object",
        "sbyte": "object",
        # "guid": "object",
        # "timespan": "object",
    }


class FakeResultProxy(object):
    """A fake class that pretends to behave like the ResultProxy.
    """

    # Object constructor
    def __init__(self, cursor, headers):
        self.fetchall = cursor.fetchall
        self.fetchmany = cursor.fetchmany
        self.rowcount = cursor.rowcount
        self.keys = lambda: headers
        self.returns_rows = True

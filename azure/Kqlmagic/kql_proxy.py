# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

import json


import six
import pandas
from .log import logger


from .display import Display


class KqlRow(six.Iterator):

    def __init__(self, row, col_num, **kwargs):
        self.kwargs = kwargs
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
            return KqlRow(s, len(s), **self.kwargs)
        else:
            return Display.to_styled_class(self.row[key], **self.kwargs)


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


class KqlRowsIter(six.Iterator):
    """ Iterator over returned rows, limited by size """

    def __init__(self, table, row_num, col_num, **kwargs):
        self.kwargs = kwargs
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
        return KqlRow(self.iter_all_iter.__next__(), self.col_num, **self.kwargs)


    def __len__(self):
        return self.rows_count


class KqlResponse(object):

    # Object constructor
    def __init__(self, response, **kwargs):
        self.json_response = response.json_response
        self.kwargs = kwargs
        self.completion_query_info = response.completion_query_info_results
        self.completion_query_resource_consumption = response.completion_query_resource_consumption_results
        self.dataSetCompletion = response.dataSetCompletion_results
        if kwargs.get("data_stream"):
            from .Kql_response_wrapper import tables_gen
            self.tables = tables_gen(response)
        else:
        self.tables =  [KqlTableResponse(t, response.visualization_results.get(t.id, {})) for t in response.primary_results]


class KqlTableResponse(object):

    def __init__(self, data_table, visualization_results: dict, **kwargs):
        self.kwargs = kwargs
        self.visualization_results = visualization_results
        self.data_table = data_table
        self.columns_count = self.data_table.columns_count


    def fetchall(self):
        return KqlRowsIter(self.data_table, self.data_table.rows_count, self.data_table.columns_count, **self.kwargs)


    def fetchmany(self, size):
        return KqlRowsIter(self.data_table, min(size, self.data_table.rows_count), self.data_table.columns_count, **self.kwargs)


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
    def visualization_properties(self):
        " returns all Visualization in result set, such as, Title, Accumulate, IsQuerySorted, Kind, Annotation, By"
        return self.visualization_results   


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

    def create_frame_from_files(self, foldername, pandas_df):
        import glob
        import dask.dataframe as dd

        files = [f for f in glob.glob(f"{foldername}/*.csv", recursive=False)]
        files = sorted(files, key = lambda x: x[:-4])
        if not pandas_df:
            return dd.read_csv(files, names=self.data_table.columns_name)
        li = []
        for filename in files:
            df = pandas.read_csv(filename, index_col=None, header=0, names=self.data_table.columns_name)
            li.append(df)
        frame = pandas.concat(li, axis=0, ignore_index=True)
        return frame

    def to_dataframe(self, raise_errors=True):
        """Returns Pandas data frame."""
        if self.data_table.columns_count == 0 or self.data_table.rows_count == 0:
            # return pandas.DataFrame()
            pass
        from .Kql_response_wrapper import CSV_table_reader
        foldername = self.data_table.rows.foldername if isinstance(self.data_table.rows, CSV_table_reader) else None
        if not foldername:
            frame = pandas.DataFrame(self.data_table.rows, columns=self.data_table.columns_name)
        else:
            frame = self.create_frame_from_files(foldername, pandas_df=True)
        for (idx, col_name) in enumerate(self.data_table.columns_name):
            col_type = self.data_table.columns_type[idx].lower()
            if col_type == "timespan":
                frame[col_name] = pandas.to_timedelta(
                    frame[col_name].apply(lambda t: t.replace(".", " days ") if t and "." in t.split(":")[0] else t)
                )
            elif col_type == "dynamic":
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

    def to_dask(self, raise_errors=True):
        """Returns Dask data frame."""
        if self.data_table.columns_count == 0 or self.data_table.rows_count == 0:
            pass
        import dask.dataframe as dd
        from .Kql_response_wrapper import CSV_table_reader
        foldername = self.data_table.rows.foldername if isinstance(self.data_table.rows, CSV_table_reader) else None
        if not foldername:
            frame = dd.from_pandas(self.to_dataframe(),npartitions=1)
            return frame
        frame = self.create_frame_from_files(foldername, pandas_df=False)

        for (idx, col_name) in enumerate(self.data_table.columns_name):
            col_type = self.data_table.columns_type[idx].lower()
            if col_type in self.KQL_TO_DATAFRAME_DATA_TYPES:
                pandas_type = self.KQL_TO_DATAFRAME_DATA_TYPES[col_type]
                # NA type promotion
                if pandas_type == "int64" or pandas_type == "int32":
                    if frame[col_name].isnull().any() or frame[col_name].isna().any() or frame[col_name].isin(["nan"]).any():
                        pandas_type = "object"
                        break

                elif pandas_type == "bool":
                    if frame[col_name].isnull().any() or frame[col_name].isna().any() or frame[col_name].isin(["nan"]).any():
                        pandas_type = "object"
                        break

                frame[col_name] = frame[col_name].astype(pandas_type)

        return frame


        
    @staticmethod
    def _dynamic_to_object(value):
        try:
            return json.loads(value) if value and isinstance(value, str) else value if value else None
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


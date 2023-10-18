# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

import functools
import operator
import csv
import codecs
import os.path
import re
import uuid
import base64
import json
import io
from typing import Any, Union, Dict

from traitlets.traitlets import Bool


from ._debug_utils import debug_print
from .dependencies import Dependencies


from .log import logger


from ._version import __version__ as kqlmagic_version
from .constants import ExtendedPropertiesKeys, VisualizationKeys, VisualizationValues, VisualizationScales, VisualizationLegends
from .my_utils import adjust_path, json_dumps
from .column_guesser import ColumnGuesserMixin
from .display import Display
from .palette import Palette, Palettes
from .ipython_api import IPythonAPI


def _unduplicate_field_names(field_names):
    """Append a number to duplicate field names to make them unique. """
    res = []
    for k in field_names:
        if k in res:
            i = 1
            while k + "_" + str(i) in res:
                i += 1
            k += "_" + str(i)
        res.append(k)
    return res


class UnicodeWriter(object):
    """
    A CSV writer which will write rows to CSV file "f",
    which is encoded in the given encoding.
    """

    # Object constructor
    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        # Redirect output to a queue
        self.queue = io.StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()


    def writerow(self, row):
        _row = row
        self.writer.writerow(_row)
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)
        self.queue.seek(0)


    def writerows(self, rows):
        for row in rows:
            self.writerow(row)


class FileResultDescriptor(bytes):
    """Provides Notebook-friendly output for the feedback after a ``.csv`` called."""

    # requires ocra
    # for eps - also requires poppler
    FILE_BINARY_FORMATS = ["png", "pdf", "jpeg", "jpg", "eps"]
    FILE_STRING_FORMATS = ["svg", "webp", "csv"]


    @staticmethod
    def get_format(file, format=None):
        if format is None and file is not None and isinstance(file, str):
            parts = file.split(".")
            if len(parts) > 1:
                f = parts[-1]
                if f in FileResultDescriptor.FILE_BINARY_FORMATS or f in FileResultDescriptor.FILE_STRING_FORMATS:
                    return f
        return format


    # Object constructor
    def __new__(cls, file_or_image, message=None, format=None, show=False):
        if isinstance(file_or_image, bytes):
            return super(FileResultDescriptor, cls).__new__(cls, file_or_image)
        else:
            return super(FileResultDescriptor, cls).__new__(cls)


    def __init__(self, file_or_image, message=None, format=None, show=False):
        if isinstance(file_or_image, bytes):
            self.show = True
        self.file_or_image = file_or_image
        self.show = show
        self.is_image = isinstance(file_or_image, bytes)
        self.message = message or ("image" if self.is_image else file_or_image)
        self.format = self.get_format(file_or_image, format)


    def _get_data(self):
        if self.is_image:
            return self if self.format in FileResultDescriptor.FILE_BINARY_FORMATS else "".join(chr(x) for x in self)
        else:
            filename = adjust_path(self.file_or_image)
            return open(filename, "rb" if self.format in self.FILE_BINARY_FORMATS else "r").read()


    def _file_location_message(self):
        file_location = os.path.join(os.path.abspath("."), self.file_or_image)
        return f"{self.message} at {file_location}"


    # Printable unambiguous presentation of the object
    def __repr__(self):
        if self.is_image:
            return "".join(chr(x) for x in self)
        elif self.show:
            return str(self._get_data())
        else:
            return self._file_location_message()


    # html presentation of the object
    def _repr_html_(self):
        if self.show and self.format == "html":
            return self._get_data()
        # if not self.show and not self.is_image:
        #     href = os.path.join(".", "files", self.file_or_image)
        #     return f'<a href="{href}" download>{self.message}</a>'


    def _repr_png_(self):
        if self.show and self.format == "png":
            # print("_repr_png_")
            return self._get_data()


    def _repr_jpeg_(self):
        if self.show and (self.format == "jpeg" or self.format == "jpg"):
            return self._get_data()


    def _repr_svg_(self):
        if self.show and self.format == "svg":
            return self._get_data()


    def _repr_webp_(self):
        if self.show and self.format == "webp":
            return self._get_data()


    def _repr_pdf_(self):
        if self.show and self.format == "pdf":
            return self._get_data()


    def _repr_eps_(self):
        if self.show and self.format == "eps":
            return self._get_data()


def _nonbreaking_spaces(match_obj):
    """
    Make spaces visible in HTML by replacing all `` `` with ``&nbsp;``

    Call with a ``re`` match object.  Retain group 1, replace group 2
    with nonbreaking speaces.
    """
    spaces = "&nbsp;" * len(match_obj.group(2))
    return f"{match_obj.group(1)}{spaces}"


class DisplayRows(list):

    def __init__(self, rows:list, limit:int):
        self.rows = [] if rows is None else rows
        self.limit = len(self.rows) if limit is None else min(max(0, limit), len(self.rows))
        self.row_index = 0


    def __getitem__(self, key):
        if isinstance(key, slice):
            if key.start and key.start >= self.limit:
                return
            key_start = key.start
            if key.stop is None or key.stop >= self.limit:
                key_stop = self.limit - 1
            else:
                key_stop = key.stop
            key_slice = slice(key_start, key_stop, key.step)
            return list.__getitem__(self.rows, key_slice)
        if key < self.limit:
            return list.__getitem__(self.rows, key)


    def __len__(self):
        return self.limit


    def __iter__(self):
        self.row_index = 0
        return self


    def __next__(self):
        if self.row_index >= self.limit:
            raise StopIteration
        val = self.__getitem__(self.row_index)
        self.row_index = self.row_index + 1
        return val


class ResultSet(list, ColumnGuesserMixin):
    """
    Results of a query.

    Can access rows listwise, or by string value of leftmost column.
    """

    _is_matplotlib_intialized = False
    matplotlib_pyplot = None


    # Object constructor
    def __init__(self, metadata:Dict[str,Any], queryResult, fork_table_id:int=0, fork_table_resultSets=None):

        #         self.current_colors_palette = ['rgb(184, 247, 212)', 'rgb(111, 231, 219)', 'rgb(127, 166, 238)', 'rgb(131, 90, 241)']

        self.fork_table_id = fork_table_id
        fork_table_resultSets = fork_table_resultSets or {}
        self._fork_table_resultSets = fork_table_resultSets
        # set by caller

        self.feedback_info = []
        self.feedback_warning = []

        self._suppress_next_repr_html_=None
        self.display_info = True
        self.suppress_result = False
        self.update_obj(metadata, queryResult)


    def _update_metadata(self, metadata:Dict[str,Any])->None:
        self._metadata = metadata
        self.parametrized_query_obj = metadata.get('parametrized_query_obj')
        self.options = metadata['parsed'].get('options') or {}
        self.engine = metadata.get('engine')


    def _get_palette(self, n_colors:int=None, desaturation:float=None):
        name = self.options.get("palette_name")
        length = max(n_colors or 10, self.options.get("palette_colors") or 10)
        self._metadata["palette"] = Palette(
            palette_name=name,
            n_colors=length,
            desaturation=desaturation or self.options.get("palette_desaturation"),
            to_reverse=self.options.get("palette_reverse"),
        )
        return self.palette


    def get_color_from_palette(self, idx:int, n_colors:int=None, desaturation:float=None)->str:
        palette = self.palette or self._get_palette(n_colors, desaturation)
        if idx < len(palette):
            return str(palette[idx])
        return None


    # Public API   
    @property 
    def parametrized_query(self)->str:
        return self.parametrized_query_obj.pretty_query


    # Public API   
    @property
    def query(self)->str:
        return self._metadata.get("parsed").get("query").strip()


    # Public API   
    @property
    def plotly_fig(self):
        return self._metadata.get("chart_figure")


    # Public API   
    @property
    def palette(self):
        return self._metadata.get("palette")


    # Public API   
    @property
    def palettes(self)->Palettes:
        return Palettes(n_colors=self.options.get("palette_colors"), desaturation=self.options.get("palette_desaturation"))


    # Public API   
    @property
    def connection(self)->str:
        return self._metadata.get("conn_name")


    # Public API   
    @property
    def start_time(self)->float:
        return self._metadata.get("start_time")


    # Public API   
    @property
    def end_time(self)->float:
        return self._metadata.get("end_time")


    # Public API   
    @property
    def elapsed_timespan(self)->float:
        return self.end_time - self.start_time


    # Public API   
    @property
    def visualization(self):
        return self.visualization_properties.get(VisualizationKeys.VISUALIZATION)


    # Public API   
    @property
    def cursor(self):
        return self._cursor


    # Public API   
    @property
    def title(self)->str:
        return self.visualization_properties.get(VisualizationKeys.TITLE)

    # Public API
    def deep_link(self, qld_param:str=None):
        if (qld_param and qld_param not in ["Kusto.Explorer", "Kusto.WebExplorer"]):
            raise ValueError('Unknow deep link destination, the only supported are: ["Kusto.Explorer", "Kusto.WebExplorer"]')
        _options = {**self.options, "query_link_destination": qld_param} if qld_param else self.options
        deep_link_url = self.engine.get_deep_link(self.parametrized_query_obj.query, options=_options)
        # only use deep links for kusto connection
        if deep_link_url is not None:
            logger().debug("ResultSet::deep_link - url: {deep_link_url}")
            qld = _options.get("query_link_destination").lower().replace('.', '_')
            close_window_timeout_in_secs = 60 if _options.get("query_link_destination") == "Kusto.Explorer" else None
            # close opening window only for Kusto.Explorer app, for Kusto.WebExplorer leave window
            html_obj = Display.get_show_deeplink_html_obj(f"query_link_{qld}", deep_link_url, close_window_timeout_in_secs, options=_options)
            return html_obj
        else:
            raise ValueError('Deep link not supported for this connection, only Azure Data Explorer connections are supported')
        return None


    def _update_query_results(self, queryResult)->None:
        self._queryResult = queryResult
        self._completion_query_info = queryResult.completion_query_info
        self._completion_query_resource_consumption = queryResult.completion_query_resource_consumption
        self._dataSetCompletion = queryResult.dataSetCompletion
        self._json_response = queryResult.json_response
        queryResultTable = queryResult.tables[self.fork_table_id]
        self._dataframe = None
        # schema
        self.columns_name = queryResultTable.keys()
        self.columns_type = queryResultTable.types()
        self.columns_datafarme_type = queryResultTable.datafarme_types
        self.field_names = _unduplicate_field_names(self.columns_name)
        self.pretty = None
        self.records_count = queryResultTable.recordscount()
        self.is_partial_table = queryResultTable.ispartial()
        self.visualization_properties = queryResultTable.extended_properties.get(ExtendedPropertiesKeys.VISUALIZATION, {})
        self._cursor = queryResultTable.extended_properties.get(ExtendedPropertiesKeys.CURSOR, "")
        # table
        auto_limit = 0 if not self.options.get("auto_limit") else self.options.get("auto_limit")
        if queryResultTable.returns_rows():
            if auto_limit > 0:
                list.__init__(self, queryResultTable.fetchmany(size=auto_limit))
            else:
                list.__init__(self, queryResultTable.fetchall())

        else:
            list.__init__(self, [])

        self._fork_table_resultSets[str(self.fork_table_id)] = self
    

    def update_obj(self, metadata:Dict[str,Any], queryResult)->None:
        self._update_metadata(metadata)
        self._update_query_results(queryResult)


    def _create_fork_results(self)->None:
        if self.fork_table_id == 0 and len(self._fork_table_resultSets) == 1:
            for fork_table_id in range(1, len(self._queryResult.tables)):
                r = ResultSet(self._metadata, self._queryResult, fork_table_id=fork_table_id, fork_table_resultSets=self._fork_table_resultSets)
                if r.options.get("feedback"):
                    if r.options.get("show_query_time"):
                        minutes, seconds = divmod(self.elapsed_timespan, 60)
                        r.feedback_info.append("Done ({:0>2}:{:06.3f}): {} records".format(int(minutes), seconds, r.records_count))


    def _update_fork_results(self)->None:
        if self.fork_table_id == 0:
            for r in self._fork_table_resultSets.values():
                if r != self:
                    r.update_obj(self._metadata, self._queryResult)
                    r.feedback_info = []
                    r.feedback_warning = []
                    r.display_info = True
                    r.suppress_result = False
                    if r.options.get("feedback"):
                        if r.options.get("show_query_time"):
                            minutes, seconds = divmod(self.elapsed_timespan, 60)
                            r.feedback_info.append("Done ({:0>2}:{:06.3f}): {} records".format(int(minutes), seconds, r.records_count))


    def fork_result(self, fork_table_id:int=0):
        # return self._fork_table_resultSets.get(str(fork_table_id))
        return self._fork_table_resultSets[str(fork_table_id)]


    @property
    def raw_json(self):
        return Display.to_json_styled_class(self._json_response, options=self.options)


    @property
    def completion_query_info(self):
        return Display.to_json_styled_class(self._completion_query_info, options=self.options)


    @property
    def completion_query_resource_consumption(self):
        return Display.to_json_styled_class(self._completion_query_resource_consumption, options=self.options)


    @property
    def dataSetCompletion(self):
        return Display.to_json_styled_class(self._dataSetCompletion, options=self.options)


    # IPython html presentation of the object
    def _repr_html_(self)->str:
        self.show_result()
        return ""


    def show_result(self, is_last_query:Bool=None)->str:
        suppress_next_repr_html_ = bool(is_last_query)  # and self.need_suppress_next_workaround()
        suppress_current_repr_html_ = self._suppress_next_repr_html_
        self._suppress_next_repr_html_ = suppress_next_repr_html_
        if suppress_current_repr_html_:
            return ""

        if not self.suppress_result:
            feedback_warning = self.feedback_warning if self.display_info else None
            conn_info = self._metadata.get("conn_info") if self.display_info else None
            parametrized_query = self.parametrized_query_obj.query if self.display_info and self.options.get("show_query") else None
            feedback_info = self.feedback_info if self.display_info else None

            Display.showWarningMessage(feedback_warning, display_handler_name='feedback_warning', **self.options)
            Display.showInfoMessage(conn_info, display_handler_name='conn_info', **self.options)
            Display.showInfoMessage(parametrized_query, display_handler_name='parametrized_query', **self.options)

            if self.is_chart():
                self.show_chart(**self.options, display_handler_name='table_or_chart')
            else:
                self.show_table(**self.options, display_handler_name='table_or_chart')

            Display.showInfoMessage(feedback_info, display_handler_name='feedback_info', **self.options)

            if self.display_info and self.options.get("show_query_link"):
                self.show_button_to_deep_link(display_handler_name='deep_link')
            else:
                Display.showInfoMessage(None, display_handler_name='deep_link', **self.options)

        else:
            Display.showWarningMessage(None, display_handler_name='feedback_warning', **self.options)
            Display.showInfoMessage(None, display_handler_name='conn_info', **self.options)
            Display.showInfoMessage(None, display_handler_name='parametrized_query', **self.options)
            Display.showInfoMessage(None, display_handler_name='table_or_chart', **self.options)
            Display.showInfoMessage(None, display_handler_name='feedback_info', **self.options)
            Display.showInfoMessage(None, display_handler_name='deep_link', **self.options)

        # display info only once
        self.display_info = False

        # suppress results info only once
        self.suppress_result = False
        return ""


    def show_button_to_deep_link(self, browser:bool=False, display_handler_name:str=None)->None:
        close_window_timeout_in_secs = 60 if self.options.get("query_link_destination") == "Kusto.Explorer" else None
        deep_link_url = self.engine.get_deep_link(self.parametrized_query_obj.query, options=self.options)

        if deep_link_url is not None:  # only use deep links for kusto connection 
            logger().debug(f"ResultSet::show_button_to_deep_link - url: {deep_link_url}")
            import urllib.parse
            # nteract cannot execute deep link script, workaround using temp_file_server webbrowser
            if self.options.get("notebook_app") in ["nteract"]:
                if self.options.get("kernel_location") != "local" or self.options.get("temp_files_server_address") is None:
                    return None
                qld = self.options.get("query_link_destination").lower().replace('.', '_')
                deep_link_url = Display.get_show_deeplink_webbrowser_html_obj(f"query_link_{qld}", deep_link_url, close_window_timeout_in_secs, options=self.options)
                deep_link_url = f'{self.options.get("temp_files_server_address")}/webbrowser?url={urllib.parse.quote(deep_link_url)}&kernelid={self.options.get("kernel_id")}'

            qld = self.options.get("query_link_destination").lower().replace('.', '_')
            Display.show_window(
                f"query_link_{qld}", 
                deep_link_url, 
                f"{self.options.get('query_link_destination')}", 
                onclick_visibility="visible",
                palette=Display.info_style,
                before_text=f"Click to execute query in {self.options.get('query_link_destination')} ",
                display_handler_name=display_handler_name,
                close_window_timeout_in_secs=close_window_timeout_in_secs,
                **self.options
            )
        return None


    def _getPrettyTableHtml(self)->Dict[str,str]:
        "get query result in a table format as an HTML string"

        display_limit = self.options.get("display_limit")
        if display_limit is None:
            display_limit = len(self)

        if display_limit >= 0:
            table = DisplayRows(self, display_limit)
            prettytable = Dependencies.get_module('prettytable', dont_throw=True)
            if prettytable:
                if self.pretty is None:
                    # table printing style to any of prettytable's defined styles (currently DEFAULT, MSWORD_FRIENDLY, PLAIN_COLUMNS, RANDOM)
                    prettytable_style = prettytable.__dict__[self.options.get("prettytable_style", "DEFAULT").upper()]
                    self.pretty = PrettyTable(self.field_names, style=prettytable_style) if len(self.field_names) > 0 else None

                self.pretty.add_rows(table)
                result = self.pretty.get_html_string()
                _cell_with_spaces_pattern = re.compile(r"(<td>)( {2,})")
                result = _cell_with_spaces_pattern.sub(_nonbreaking_spaces, result)
            else:
                tabulate = Dependencies.get_module('tabulate', dont_throw=True)
                if tabulate:
                    result = tabulate.tabulate(table, self.field_names, tablefmt="html")
                else:
                    prettytable = Dependencies.get_module('prettytable')
                    return {}

            if len(self) > display_limit:
                result = f'{result}\n<span style="font-style:italic;text-align:center;">{len(self)} rows, truncated to display_limit of {display_limit}</span>'
            
            return {"body": result}
        else:
            return {}



    def need_suppress_next_workaround(self)->bool:
        if self.options.get("table_package") != "pandas_html_table_schema" \
                or self.options.get("popup_window") \
                or (len(self) == 1 and len(self[0]) == 1 and (isinstance(self[0][0], dict) or isinstance(self[0][0], list))):
            return False
        else:
            return False


    def show_table(self, display_handler_name:str=None, **kwargs)->None:
        "display the table"

        options = {**self.options, **kwargs}

        pd = None
        pandas__repr_data_resource_ = None
        pandas_display_html_table_schema = None
        pandas__repr_data_resource_patched = False
        if not options.get("popup_window") and len(self) == 1 and len(self[0]) == 1 and (isinstance(self[0][0], dict) or isinstance(self[0][0], list)):
            content = Display.to_json_styled_class(self[0][0], options=options)
        else:
            display_limit = options.get("display_limit")
            if type(display_limit) == int and display_limit < 0:
                if options.get("notebook_app") in ["azuredatastudiosaw"] and options.get("popup_window"):
                    content = ""
                else:
                    content = Display.toHtml(**{}, title='table')

            elif options.get("table_package", "").lower() in ["pandas", "pandas_html_table_schema"]:
                pd = Dependencies.get_module("pandas")

                df = self.to_dataframe()
                if display_limit is not None:
                    df = df.head(display_limit)

                pd.set_option('display.max_rows', display_limit)
                pd.set_option('display.max_columns', None)
                try:
                    pd.set_option('display.min_rows', display_limit)
                except:
                    pass
                pd.set_option('display.large_repr', "truncate")

                if options.get("table_package", "") == "pandas_html_table_schema" and not options.get("popup_window"):
                    df_copied = False
                    for idx, column_type in enumerate(self.columns_type):
                        if column_type == "dynamic":
                            if not df_copied:
                                df_copied =  True
                                df = df.copy()
                            col_name = self.columns_name[idx]
                            for item_idx, item in enumerate(df[col_name]):
                                df.loc[item_idx, col_name] = f"{item}"

                    pandas_display_html_table_schema = pd.options.display.html.table_schema
                    pd.options.display.html.table_schema = True
                    if options.get("notebook_app") in ["azuredatastudio", "azuredatastudiosaw"]:
                        pandas__repr_data_resource_ = self._patch_pandas__repr_data_resource_()
                        pandas__repr_data_resource_patched = True
                    content = df

                else:
                    pandas_display_html_table_schema = pd.options.display.html.table_schema
                    pd.options.display.html.table_schema = False
                    if options.get("notebook_app") in ["azuredatastudiosaw"] and options.get("popup_window"):
                        content = df.to_string()
                    else:
                        t = df._repr_html_()
                        content = Display.toHtml(body=t, title='table')
            # prettytable or tabulate in html format
            else:
                t = self._getPrettyTableHtml()
                content = Display.toHtml(**t, title='table')

        if options.get("popup_window") and not options.get("button_text"):
            options["button_text"] = f'popup table{((" - " + self.title) if self.title else "")} '

        Display.show(content, display_handler_name=display_handler_name, **options)

        #
        # restore pandas state
        #
        if pandas__repr_data_resource_patched and pandas__repr_data_resource_ is not None:
            self._unpatch_pandas__repr_data_resource_(pandas__repr_data_resource_)
        if pandas_display_html_table_schema is not None:
            pd.options.display.html.table_schema = pandas_display_html_table_schema

        return None


    def _patch_pandas__repr_data_resource_(self):
        "patch pandas' _repr_data_resource_ method. main modifications is to remove pandas primary key index"
        pd = Dependencies.get_module("pandas")
        pandas__repr_data_resource_ = pd.DataFrame._repr_data_resource_

        def my__repr_data_resource_(pandas_self):
            "replace pandas method. Takes pandas method result and modify it"

            # object from pandas
            obj = pandas__repr_data_resource_(pandas_self)
            modified_obj = False

            try: 
                if not isinstance(obj, dict):
                    return obj

                schema = obj.get("schema")
                if not isinstance(schema, dict):
                    return obj

                #
                # mofify schema part
                #
                primary_keys = schema.get('primaryKey')
                if isinstance(primary_keys, str):
                    primary_keys = [primary_keys.strip()]

                if not isinstance(primary_keys, list):
                    return obj

                if 'index' not in primary_keys:
                    return obj

                fields = schema.get("fields")
                if not isinstance(fields, list):
                    return obj

                rows = obj.get("data")
                if not isinstance(rows, list):
                    return obj

                #
                # mofify schema
                #

                # remove primary key, becuase index is the primary key
                del schema["primaryKey"]

                # replace pandas version with kqlmagic version
                if "pandas_version" in schema:
                    del schema["pandas_version"]

                schema["kqmagic_version"] = kqlmagic_version

                # remove 'index' field metadata
                fields = schema.get("fields")
                for idx, field in enumerate(fields):
                    if field.get("name") == "index":
                        fields.pop(idx)
                        break

                #
                # mofify data
                #

                # remove 'index' column
                for row in rows:
                    del row["index"]

            except:
                pass
            if not modified_obj:
                logger().debug(f"ResultSet::my__repr_data_resource_ - didn't modify:\n {obj}\n")
            return obj


        pd.DataFrame._repr_data_resource_ = my__repr_data_resource_

        return pandas__repr_data_resource_


    def _unpatch_pandas__repr_data_resource_(self, pandas__repr_data_resource_)->None:
        pd = Dependencies.get_module("pandas")
        pd.DataFrame._repr_data_resource_ = pandas__repr_data_resource_


    # Public API   
    def popup_table(self, **kwargs)->None:
        "display the table in popup window"
        self.show_table(**{"popup_window": True, **kwargs})


    # Public API   
    def display_table(self, **kwargs)->None:
        "display the table in cell"
        self.show_table(**{"popup_window": False, **kwargs})


    # Printable pretty presentation of the object
    def __str__(self, *args, **kwargs)->str:
        j_table = [[json.loads(json_dumps(row[col])) for col in self.columns_name] for row in self]
        return json_dumps(j_table)


    # For iterator self[key]
    def __getitem__(self, key:Union[int,str,slice]):
        """
        Access by integer (row position within result set)
        or by string (value of leftmost column)
        """
        try:
            item = list.__getitem__(self, key)
        except TypeError:
            result = [row for row in self if row[0] == key]
            if not result or len(result) == 0:
                raise KeyError(key)
            if len(result) > 1:
                raise KeyError(f"{len(result)} results for '{key}'")
            item = result[0]

        if isinstance(key, slice):
            if key.start is None and key.stop is None and key.step is None:
                return item
            elif len(item) == 0:
                return item

        return Display.to_json_styled_class(item, options=self.options)


    def to_dict(self)->Dict[str,Any]:
        """Returns a single dict built from the result set
        Keys are column names; values are a tuple"""
        if len(self):
            return dict(zip(self.columns_name, zip(*self)))
        else:
            return dict(zip(self.columns_name, [() for c in self.columns_name]))


    def dicts_iterator(self):
        "Iterator yielding a dict for each row"
        for row in self:
            yield dict(zip(self.columns_name, row))


    # Public API   
    def to_dataframe(self):
        "Returns a Pandas DataFrame instance built from the result set."
        if self._dataframe is None:
            self._dataframe = self._queryResult.tables[self.fork_table_id].to_dataframe(options=self.options)

            # pd = Dependencies.get_module("pandas")
            # frame = pd.DataFrame(self, columns=(self and self.columns_name) or [])
            # self._dataframe = frame
        return self._dataframe


    # Public API   
    def submit(self, override_vars:Dict[str,str]=None, override_options:Dict[str,Any]=None, override_query_properties:Dict[str,Any]=None, override_connection:str=None)->None:
        "execute the query again"
        magic = self._metadata.get("magic")
        line = self._metadata.get("parsed").get("line")
        cell = self._metadata.get("parsed").get("cell")

        return magic.execute(
            line, cell, 
            override_vars=override_vars, 
            override_options=override_options, 
            override_query_properties=override_query_properties,
            override_connection=override_connection)


    # Public API   
    def refresh(self, override_vars:Dict[str,str]=None, override_options:Dict[str,Any]=None, override_query_properties:Dict[str,Any]=None, override_connection:str=None)->None:
        "refresh the results of the query, on the same object: self"

        _override_options = {**override_options} if type(override_options) == dict else {}
        if _override_options.get('display_id') is None or _override_options.get('display_id') == self.options.get('display_id'):
            _override_options['display_handlers'] = self.options.get('display_handlers')

        magic = self._metadata.get("magic")
        line = self._metadata.get("parsed").get("line")
        cell = self._metadata.get("parsed").get("cell")

        return magic.execute(
            line, cell, 
            override_vars=override_vars, 
            override_options=_override_options, 
            override_query_properties=override_query_properties,
            override_connection=override_connection,
            override_result_set=self)


    # Public API   
    def popup(self, **kwargs)->None:
        if self.is_chart():
            self.popup_Chart(**kwargs)
        else:
            self.popup_table(**kwargs)


    # Public API        
    def show_chart(self, display_handler_name:str=None, **kwargs)->None:
        "display the chart that was specified in the query"
        _options = {**self.options, **kwargs}
        window_mode = _options.get("popup_window")

        if window_mode and not _options.get("button_text"):
            _options["button_text"] = "popup " + self.visualization + ((" - " + self.title) if self.title else "") + " "
        c = self._getChartHtml(window_mode, options=_options)
        if c.get("body") or c.get("head"):
            html = Display.toHtml(**c, title='chart')
            Display.show(html, display_handler_name=display_handler_name, **_options)
        elif c.get("fig"):
            if _options.get("notebook_app") in ["azurenotebook", "azureml", "azuremljupyternotebook", "azuremljupyterlab", "jupyterlab", "visualstudiocode", "ipython"]:
                plotly = Dependencies.get_module('plotly')
                plotly.offline.init_notebook_mode(connected=True)
                plotly.offline.iplot(c.get("fig"), filename="plotlychart")
            else:
                Display.show(c.get("fig"), display_handler_name=display_handler_name, **_options)
        else:
            return self.show_table(**kwargs)


    # Public API 
    def to_image(self, **kwargs)->FileResultDescriptor:
        "export image of the chart that was specified in the query to a file"
        _options = {**self.options, **kwargs}

        if self.options.get("plot_package") in ["plotly_orca", "plotly", "plotly_widget"]:

            # replace rendering to plotly, to make it work with _plotly_fig_to_image() below
            _options = {**self.options, **{"plot_package": "plotly"}}
            fig = self._getChartHtml(window_mode=False, options=_options).get("fig")
            if fig is not None:

                filename = adjust_path(kwargs.get("filename"))
                if filename is not None:
                    file_or_image_bytes = self._plotly_fig_to_image(fig, filename, options=_options)

                    if file_or_image_bytes:

                        return FileResultDescriptor(file_or_image_bytes, message="image results", format=_options.get("format"), show=_options.get("show"))


    # Public API 
    def popup_Chart(self, **kwargs)->None:
        "display the chart that was specified in the query in a popup window"
        self.chart_popup(**kwargs)


    def display_Chart(self, **kwargs)->None:
        "display the chart that was specified in the query in the cell"
        self.chart_display(**kwargs)


    def chart_popup(self, **kwargs)->None:
        "display the chart that was specified in the query"
        self.show_chart(**{"popup_window": True, **kwargs})


    def chart_display(self, **kwargs)->None:
        "display the chart that was specified in the query"
        self.show_chart(**{"popup_window": False, **kwargs})


    def is_chart(self)->bool:
        return self.visualization and self.visualization != VisualizationValues.TABLE


    _SUPPORTED_PLOT_PACKAGES = [
        "plotly",
        "plotly_orca",
        "plotly_widget"
    ]


    def _getChartHtml(self, window_mode:bool=False, options:Dict[str,Any]=None)->Dict[str,Any]:
        "get query result in a char format as an HTML string"
        # https://kusto.azurewebsites.net/docs/queryLanguage/query_language_renderoperator.html

        options = options or {}
        if not self.is_chart():
            return {}

        if options.get("plot_package") == "None":
            return {}

        if options.get("plot_package") not in self._SUPPORTED_PLOT_PACKAGES:
            return {}

        if len(self) == 0:
            id = uuid.uuid4().hex
            head = (
                f"""<style>#uuid-{id} {{
                    display: block; 
                    font-style:italic;
                    font-size:300%;
                    text-align:center;
                }} </style>"""
            )

            body = f'<div id="uuid-{id}"><br><br>EMPTY CHART (no data)<br><br>.</div>'
            return {"body": body, "head": head}

        chart_obj = None

        # First column is color-axis, second column is numeric
        if self.visualization == VisualizationValues.PIE_CHART:
            chart_obj = self._render_piechart_plotly(self.visualization_properties, " ", options=options)

        # First column is x-axis, and can be text, datetime or numeric. Other columns are numeric, displayed as horizontal strips.
        # kind = default, unstacked, stacked, stacked100 (Default, same as unstacked; unstacked - Each "area" to its own; stacked 
        # - "Areas" are stacked to the right; stacked100 - "Areas" are stacked to the right, and stretched to the same width)
        elif self.visualization == VisualizationValues.BAR_CHART:
            chart_obj = self._render_barchart_plotly(self.visualization_properties, " ", options=options)

        # Like barchart, with vertical strips instead of horizontal strips.
        # kind = default, unstacked, stacked, stacked100
        elif self.visualization == VisualizationValues.COLUMN_CHART:
            chart_obj = self._render_barchart_plotly(self.visualization_properties, " ", options=options)

        # Area graph. First column is x-axis, and should be a numeric column. Other numeric columns are y-axes.
        # kind = default, unstacked, stacked, stacked100
        elif self.visualization == VisualizationValues.AREA_CHART:
            chart_obj = self._render_areachart_plotly(self.visualization_properties, " ", options=options)

        # Line graph. First column is x-axis, and should be a numeric column. Other numeric columns are y-axes.
        elif self.visualization == VisualizationValues.LINE_CHART:
            chart_obj = self._render_linechart_plotly(self.visualization_properties, " ", options=options)

        # Line graph. First column is x-axis, and should be datetime. Other columns are y-axes.
        elif self.visualization == VisualizationValues.TIME_CHART:
            chart_obj = self._render_timechart_plotly(self.visualization_properties, " ", options=options)

        # Similar to timechart, but highlights anomalies using an external machine-learning service.
        elif self.visualization == VisualizationValues.ANOMALY_CHART:
            chart_obj = self._render_linechart_plotly(self.visualization_properties, " ", options=options)

        # Stacked area graph. First column is x-axis, and should be a numeric column. Other numeric columns are y-axes.
        elif self.visualization == VisualizationValues.STACKED_AREA_CHART:
            chart_obj = self._render_stackedareachart_plotly(self.visualization_properties, " ", options=options)

        # Last two columns are the x-axis, other columns are y-axis.
        elif self.visualization == VisualizationValues.LADDER_CHART:
            # not supported yet
            return {}

        # Interactive navigation over the events time-line (pivoting on time axis)
        elif self.visualization == VisualizationValues.TIME_PIVOT:
            # not supported yet
            return {}

        # Displays a pivot table and chart. User can interactively select data, columns, rows and various chart types.
        elif self.visualization == VisualizationValues.PIVOT_CHART:
            # not supported yet
            return {}

        # Points graph. First column is x-axis, and should be a numeric column. Other numeric columns are y-axes
        elif self.visualization == VisualizationValues.SCATTER_CHART:
            chart_obj = self._render_scatterchart_plotly(self.visualization_properties, " ", options=options)

        if chart_obj is None:
            return {}

        chart_figure = self._figure_or_figurewidget(data=chart_obj.get("data"), layout=chart_obj.get("layout"), window_mode=window_mode, options=options)
        if chart_figure is not None:
            self._metadata["chart_figure"] = chart_figure
            if options.get("plot_package") == "plotly_orca":
                image_bytes = self._plotly_fig_to_image(chart_figure, None, options=options)
                image_base64_bytes= base64.b64encode(image_bytes)
                image_base64_str = image_base64_bytes.decode("utf-8")
                image_html_str = f"""<div><img src='data:image/png;base64,{image_base64_str}'></div>"""
                return {"body": image_html_str}
                
            elif window_mode:
                head = '<script src="https://cdn.plot.ly/plotly-latest.min.js"></script>' if not self.options.get("plotly_fs_includejs") else None
                plotly = Dependencies.get_module('plotly')
                body = plotly.offline.plot(
                    chart_figure,
                    include_plotlyjs=window_mode and self.options.get("plotly_fs_includejs", False), 
                    output_type="div"
                )
                return {"body": body, "head": head}

            else:
                return {"fig": chart_figure}
        return {}


    def _plotly_fig_to_image(self, fig, filename:str, options:Dict[str,Any]=None)->bytes:
        options = options or {}
        try:
            if filename:  # requires plotly orca package
                fig.write_image(
                    adjust_path(filename),
                    format=options.get("format"), 
                    scale=options.get("scale"), width=options.get("width"), height=options.get("height")
                )
                # plotly = Dependencies.get_module('plotly')
                # plotly.io.write_image(
                #     fig, file, format=options.get("format"), scale=options.get("scale"), width=options.get("width"), height=options.get("height")
                # )
                return filename        
            else:
                plotly = Dependencies.get_module('plotly')
                return plotly.io.to_image(
                    fig, format=options.get("format"), scale=options.get("scale"), width=options.get("width"), height=options.get("height")
                )
        except:
            # display image with 'orca is missing'
            plotly_orca_is_missing_base64_png: str = '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'
            plotly_orca_is_missing_bytes_png: bytes = base64.b64decode(plotly_orca_is_missing_base64_png)
            return plotly_orca_is_missing_bytes_png


    @classmethod
    def _init_matplotlib(cls, **options):
        if not cls._is_matplotlib_intialized:
            plt = Dependencies.get_module('matplotlib.pyplot')
            cls.matplotlib_pyplot = plt
            logger().debug("ResultSet::_init_matplotlib - initialize matplotlib")
            IPythonAPI.try_init_ipython_matplotlib_magic(**options)

            cls._is_matplotlib_intialized = True
        return cls.matplotlib_pyplot


    def pie(self, properties:Dict[str,Any], key_word_sep:str=" ", **kwargs):
        """Generates a pylab pie chart from the result set.

        ``matplotlib`` must be installed, and in an
        IPython Notebook, inlining must be on::

            %%matplotlib inline

        Values (pie slice sizes) are taken from the
        rightmost column (numerical values required).
        All other columns are used to label the pie slices.

        Parameters
        ----------
        key_word_sep: string used to separate column values
                      from each other in pie labels
        title: Plot title, defaults to name of value column

        Any additional keyword arguments will be passsed
        through to ``matplotlib.pylab.pie``.
        """

        plt = self._init_matplotlib(**kwargs)

        self.build_columns()

        pie = plt.pie(self.columns[1], labels=self.columns[0], **kwargs)
        plt.title(properties.get(VisualizationKeys.TITLE) or self.columns[1].name)
        plt.show()
        return pie


    def plot(self, properties:Dict[str,Any], **kwargs):
        """Generates a pylab plot from the result set.

        ``matplotlib`` must be installed, and in an
        IPython Notebook, inlining must be on::

            %%matplotlib inline

        The first and last columns are taken as the X and Y
        values.  Any columns between are ignored.

        Parameters
        ----------
        title: Plot title, defaults to names of Y value columns

        Any additional keyword arguments will be passsed
        through to ``matplotlib.pylab.plot``.
        """
        plt = self._init_matplotlib(**kwargs)

        self.guess_plot_columns()
        self.x = self.x or range(len(self.ys[0]))
        coords = functools.reduce(operator.add, [(self.x, y) for y in self.ys])
        plot = plt.plot(*coords, **kwargs)
        if hasattr(self.x, "name"):
            plt.xlabel(self.x.name)
        ylabel = ", ".join(y.name for y in self.ys)
        plt.title(properties.get(VisualizationKeys.TITLE) or ylabel)
        plt.ylabel(ylabel)
        return plot


    def bar(self, properties:Dict[str,Any], key_word_sep:str=" ", **kwargs):
        """Generates a pylab bar plot from the result set.

        ``matplotlib`` must be installed, and in an
        IPython Notebook, inlining must be on::

            %%matplotlib inline

        The last quantitative column is taken as the Y values;
        all other columns are combined to label the X axis.

        Parameters
        ----------
        title: Plot title, defaults to names of Y value columns
        key_word_sep: string used to separate column values
                      from each other in labels

        Any additional keyword arguments will be passsed
        through to ``matplotlib.pylab.bar``.
        """
        plt = self._init_matplotlib(**kwargs)

        self.guess_pie_columns(xlabel_sep=key_word_sep)
        plot = plt.bar(range(len(self.ys[0])), self.ys[0], **kwargs)
        if self.xlabels:
            plt.xticks(range(len(self.xlabels)), self.xlabels, rotation=45)
        plt.xlabel(self.xlabel)
        plt.ylabel(self.ys[0].name)
        return plot


    def to_csv(self, filename:str=None, **kwargs):
        """Generate results in comma-separated form.  Write to ``filename`` if given.
           Any other parameters will be passed on to csv.writer."""
        if len(self) == 0:
            return None  # no results
        if filename:
            filename = adjust_path(filename)
            encoding = kwargs.get("encoding", "utf-8")
            outfile = open(filename, "w", newline="", encoding=encoding)
        else:
            outfile = io.StringIO()
        writer = UnicodeWriter(outfile, **kwargs)
        writer.writerow(self.field_names)
        for row in self:
            j_row = [json.loads(json_dumps(row[col])) for col in self.columns_name]
            writer.writerow(j_row)
        if filename:
            outfile.close()
            message = "csv results"
            return FileResultDescriptor(filename, message=message, format="csv", **kwargs)
        else:
            return outfile.getvalue()


    def _render_pie(self, properties:Dict[str,Any], key_word_sep:str=" ", **kwargs):
        """Generates a pylab pie chart from the result set.

        ``matplotlib`` must be installed, and in an
        IPython Notebook, inlining must be on::

            %%matplotlib inline

        First column is color-axis, second column is numeric

        Parameters
        ----------
        key_word_sep: string used to separate column values
                      from each other in pie labels
        title: Plot title, defaults to name of value column

        Any additional keyword arguments will be passsed
        through to ``matplotlib.pylab.pie``.
        """

        plt = self._init_matplotlib(**kwargs)

        self.build_columns()

        pie = plt.pie(self.columns[1], labels=self.columns[0], **kwargs)
        plt.title(properties.get(VisualizationKeys.TITLE) or self.columns[1].name)
        plt.show()
        return pie


    def _render_barh(self, properties:Dict[str,Any], key_word_sep:str=" ", **kwargs):
        """Generates a pylab horizaontal barchart from the result set.

        ``matplotlib`` must be installed, and in an
        IPython Notebook, inlining must be on::

            %%matplotlib inline

        First column is x-axis, and can be text, datetime or numeric. 
        Other columns are numeric, displayed as horizontal strips.

        Parameters
        ----------
        key_word_sep: string used to separate column values
                      from each other in pie labels
        title: Plot title, defaults to name of value column

        Any additional keyword arguments will be passsed
        through to ``matplotlib.pylab.pie``.
        """

        barchart = None
        plt = self._init_matplotlib(**kwargs)

        self.build_columns()
        quantity_columns = [c for c in self.columns[1:] if c.is_quantity]
        ylabel = ", ".join([c.name for c in quantity_columns])
        xlabel = self.columns[0].name

        dim = len(quantity_columns)
        w = 0.8
        dimw = w / dim

        x = plt.arange(len(self.columns[0]))
        xpos = -dimw * (len(quantity_columns) / 2)
        for y in quantity_columns:
            barchart = plt.barh(x + xpos, y, align="center", **kwargs)
            # ax.barh(x + xpos, y, width = dimw, color='b', align='center', **kwargs)
            # ax.bar(y, width = dimw, height = w, x + xpos, *, align='center', **kwargs)
            xpos += dimw
        plt.yticks(range(len(self.columns[0])), self.columns[0], rotation=0)
        plt.ylabel(xlabel)
        plt.xlabel(ylabel)
        plt.title(properties.get(VisualizationKeys.TITLE) or ylabel)

        plt.show()
        return barchart


    def _render_bar(self,properties:Dict[str,Any], key_word_sep:str=" ", **kwargs):
        """Generates a pylab horizaontal barchart from the result set.

        ``matplotlib`` must be installed, and in an
        IPython Notebook, inlining must be on::

            %%matplotlib inline

        First column is x-axis, and can be text, datetime or numeric. 
        Other columns are numeric, displayed as horizontal strips.

        Parameters
        ----------
        key_word_sep: string used to separate column values
                      from each other in pie labels
        title: Plot title, defaults to name of value column

        Any additional keyword arguments will be passsed
        through to ``matplotlib.pylab.pie``.
        """

        columnchart = None
        plt = self._init_matplotlib(**kwargs)

        self.build_columns()
        quantity_columns = [c for c in self.columns[1:] if c.is_quantity]
        ylabel = ", ".join([c.name for c in quantity_columns])
        xlabel = self.columns[0].name

        dim = len(quantity_columns)
        w = 0.8
        dimw = w / dim

        x = plt.arange(len(self.columns[0]))
        xpos = -dimw * (len(quantity_columns) / 2)
        for y in quantity_columns:
            columnchart = plt.bar(x + xpos, y, width=dimw, align="center", **kwargs)
            xpos += dimw
        plt.xticks(range(len(self.columns[0])), self.columns[0], rotation=45)
        plt.xlabel(xlabel)
        plt.ylabel(ylabel)
        plt.title(properties.get(VisualizationKeys.TITLE) or ylabel)

        plt.show()
        return columnchart


    def _render_linechart(self, properties:Dict[str,Any], key_word_sep:str=" ", **kwargs):
        """Generates a pylab plot from the result set.

        ``matplotlib`` must be installed, and in an
        IPython Notebook, inlining must be on::

            %%matplotlib inline

        First column is x-axis, and should be a numeric column. Other numeric columns are y-axes.

        Parameters
        ----------
        title: Plot title, defaults to names of Y value columns

        Any additional keyword arguments will be passsed
        through to ``matplotlib.pylab.plot``.
        """

        plt = self._init_matplotlib(**kwargs)

        self.build_columns()
        quantity_columns = [c for c in self.columns if c.is_quantity]
        if len(quantity_columns) < 2:
            return None
        x = quantity_columns[0]
        ys = quantity_columns[1:]
        ylabel = ", ".join([c.name for c in ys])
        xlabel = x.name

        coords = functools.reduce(operator.add, [(x, y) for y in ys])
        plot = plt.plot(*coords, **kwargs)
        plt.title(properties.get(VisualizationKeys.TITLE) or ylabel)
        plt.xlabel(xlabel)
        plt.ylabel(ylabel)
        plt.show()
        return plot


    def _get_plotly_axis_scale(self, specified_property:str, col=None)->str:
        if col is not None:
            if not col.is_quantity:
                return "category"
            elif col.is_datetime:
                return "date"
        return "log" if specified_property == VisualizationScales.LOG else "linear"


    def _get_plotly_ylabel(self, specified_property:str, tabs:list)->str:
        label = specified_property
        if label is None:
            label_names = []
            for tab in tabs:
                if tab.col_y.name not in label_names:
                    label_names.append(tab.col_y.name)
            label =  ", ".join(label_names)
        return label
    

    _CHART_X_TYPE = {
        VisualizationValues.TABLE: "first",
        VisualizationValues.PIE_CHART: "first",
        VisualizationValues.BAR_CHART: "first",
        VisualizationValues.COLUMN_CHART: "first",
        VisualizationValues.AREA_CHART: "quantity",
        VisualizationValues.LINE_CHART: "quantity",
        VisualizationValues.TIME_CHART: "datetime",
        VisualizationValues.ANOMALY_CHART: "datetime",
        VisualizationValues.STACKED_AREA_CHART: "quantity",
        VisualizationValues.LADDER_CHART: "last2",
        VisualizationValues.TIME_PIVOT: "datetime",
        VisualizationValues.PIVOT_CHART: "first",
        VisualizationValues.SCATTER_CHART: "quantity",
    }


    def _get_plotly_chart_x_type(self, properties:Dict[str,Any])->str:
        return self._CHART_X_TYPE.get(properties.get(VisualizationKeys.VISUALIZATION), "first")
    

    def _get_plotly_chart_properties(self, properties:Dict[str,Any], tabs: list, options:Dict[str,Any]=None)->Dict[str,Any]:
        options = options or {}
        chart_properties = {}
        if properties.get(VisualizationKeys.VISUALIZATION) == VisualizationValues.BAR_CHART:
            chart_properties["xlabel"] = self._get_plotly_ylabel(properties.get(VisualizationKeys.X_TITLE), tabs)
            chart_properties["ylabel"] = properties.get(VisualizationKeys.Y_TITLE) or tabs[0].col_x.name
            chart_properties["yscale"] = self._get_plotly_axis_scale(properties.get(VisualizationKeys.Y_AXIS), tabs[0].col_x)
            chart_properties["xscale"] = self._get_plotly_axis_scale(properties.get(VisualizationKeys.X_AXIS))
            chart_properties["orientation"] = "h"
        else:
            chart_properties["ylabel"] = self._get_plotly_ylabel(properties.get(VisualizationKeys.Y_TITLE), tabs)
            chart_properties["xlabel"] = properties.get(VisualizationKeys.X_TITLE) or tabs[0].col_x.name
            chart_properties["xscale"] = self._get_plotly_axis_scale(properties.get(VisualizationKeys.X_AXIS), tabs[0].col_x)
            chart_properties["yscale"] = self._get_plotly_axis_scale(properties.get(VisualizationKeys.Y_AXIS))
            chart_properties["orientation"] = "v"
        chart_properties["autorange"] = "reversed" if tabs[0].is_descending_sorted else True
        chart_properties["showlegend"] = properties.get(VisualizationKeys.LEGEND) != VisualizationLegends.HIDDEN
        chart_properties["title"] = properties.get(VisualizationKeys.TITLE) or properties.get(VisualizationKeys.VISUALIZATION)
        chart_properties["n_colors"] = len(tabs)
        if tabs[0].col_y_min is not None and tabs[0].col_y_max is not None:
            chart_properties["range"] = [tabs[0].col_y_min, tabs[0].col_y_max]
        return chart_properties


    def _figure_or_figurewidget(self, data, layout:Dict[str,Any], window_mode:bool, options:dict=None):
        options = options or {}
        plotly_layout = options.get("plotly_layout")
        if plotly_layout is not None:
            for property in plotly_layout:
                layout[property] = plotly_layout.get(property)

        go = Dependencies.get_module('plotly.graph_objs')
        if IPythonAPI.is_ipywidgets_installed() and options.get("plot_package") == "plotly_widget" and not window_mode:
            # print("----------- FigureWidget --------------")

            fig = go.FigureWidget(data=data, layout=layout)
        else:
            # print("----------- Figure --------------")
            fig = go.Figure(data=data, layout=layout)
        return fig


    def _render_areachart_plotly(self, properties:Dict[str,Any], key_word_sep:str=" ", options:Dict[str,Any]=None)->Dict[str,Any]:
        """Generates a pylab plot from the result set.

        Area graph. 
        First column is x-axis, and should be a numeric column. Other numeric columns are y-axes.
        """
        
        options = options or {}
        go = Dependencies.get_module('plotly.graph_objs')
        self._build_chart_sub_tables(properties, x_type=self._get_plotly_chart_x_type(properties))
        if len(self.chart_sub_tables) < 1:
            return None
        chart_properties = self._get_plotly_chart_properties(properties, self.chart_sub_tables, options=options)


        data = [
            go.Scatter(
                x=list(tab.keys()),
                y=list(tab.values()),
                name=tab.name,
                mode="lines",
                line=dict(width=0.5, color=self.get_color_from_palette(idx, n_colors=chart_properties.get("n_colors"))),
                fill="tozeroy",
            )
            for idx, tab in enumerate(self.chart_sub_tables)
        ]
        layout = go.Layout(
            title=chart_properties.get("title"),
            showlegend=chart_properties.get("showlegend"),
            xaxis=dict(
                title=chart_properties.get("xlabel"), 
                type=chart_properties.get("xscale"),
                autorange=chart_properties.get("autorange"),
            ),
            yaxis=dict(
                title=chart_properties.get("ylabel"),
                type=chart_properties.get("yscale"),
                range=chart_properties.get("range"),
                # dtick=20,
                ticksuffix="",
            ),
        )
        return {"data": data, "layout": layout}


    def _render_stackedareachart_plotly(self, properties:Dict[str,Any], key_word_sep:str=" ", options:Dict[str,Any]=None)->Dict[str,Any]:
        """Generates a pylab plot from the result set.

        Stacked area graph. 
        First column is x-axis, and should be a numeric column. Other numeric columns are y-axes.
        """

        options = options or {}
        go = Dependencies.get_module('plotly.graph_objs')
        self._build_chart_sub_tables(properties, x_type=self._get_plotly_chart_x_type(properties))
        if len(self.chart_sub_tables) < 1:
            return None
        chart_properties = self._get_plotly_chart_properties(properties, self.chart_sub_tables, options=options)

        # create stack one on top of each other
        # TODO: chack newer version of plotly, support it better
        ys_stcks = []
        y_stck = [0 for x in range(len(self.chart_sub_tables[0]))]
        for tab in self.chart_sub_tables:
            y_stck = [(r or 0) + y_stck[idx] for (idx, r) in enumerate(tab.values())]
            ys_stcks.append(y_stck)

        data = [
            go.Scatter(
                x=list(tab.keys()),
                y=ys_stcks[idx],
                name=tab.name,
                mode="lines",
                line=dict(width=0.5, color=self.get_color_from_palette(idx, n_colors=chart_properties.get("n_colors"))),
                fill="tonexty",
            )
            for idx, tab in enumerate(self.chart_sub_tables)
        ]
        layout = go.Layout(
            title=chart_properties.get("title"),
            showlegend=chart_properties.get("showlegend"),
            xaxis=dict(
                title=chart_properties.get("xlabel"), 
                type=chart_properties.get("xscale"),
                autorange=chart_properties.get("autorange"),
            ),
            yaxis=dict(
                title=chart_properties.get("ylabel"),
                type=chart_properties.get("yscale"),
                range=chart_properties.get("range"),
                # dtick=20,
                ticksuffix="",
            ),
        )
        return {"data": data, "layout": layout}


    def _render_timechart_plotly(self, properties:Dict[str,Any], key_word_sep:str=" ", options:Dict[str,Any]=None)->Dict[str,Any]:
        """Generates a pylab plot from the result set.

        Line graph. 
        First column is x-axis, and should be datetime. Other columns are y-axes.
        """

        options = options or {}
        go = Dependencies.get_module('plotly.graph_objs')
        self._build_chart_sub_tables(properties, x_type=self._get_plotly_chart_x_type(properties))
        if len(self.chart_sub_tables) < 1:
            return None
        chart_properties = self._get_plotly_chart_properties(properties, self.chart_sub_tables, options=options)

        data = [
            go.Scatter(
                x=list(tab.keys()),
                y=list(tab.values()),
                name=tab.name,
                line=dict(width=1, color=self.get_color_from_palette(idx, n_colors=chart_properties.get("n_colors"))),
                opacity=0.8,
            )
            for idx, tab in enumerate(self.chart_sub_tables)
        ]

        layout = go.Layout(
            title=chart_properties.get("title"),
            showlegend=chart_properties.get("showlegend"),
            xaxis=dict(
                rangeselector=dict(
                    buttons=list(
                        [
                            # dict(count=1, label="1m", step="month", stepmode="backward"),
                            # dict(count=6, label="6m", step="month", stepmode="backward"),
                            # dict(step="all"),
                        ]
                    )
                ),
                rangeslider=dict(),
                title=chart_properties.get("xlabel"),
                type=chart_properties.get("xscale"),
                autorange=chart_properties.get("autorange"),
            ),
            yaxis=dict(
                title=chart_properties.get("ylabel"),
                type=chart_properties.get("yscale"),
                range=chart_properties.get("range"),
                # dtick=20,
                ticksuffix="",
            ),
        )
        return {"data": data, "layout": layout}


    def _render_piechart_plotly(self, properties:Dict[str,Any], key_word_sep:str=" ", options:Dict[str,Any]=None)->Dict[str,Any]:
        """Generates a pylab plot from the result set.

        Pie chart. 
        First column is color-axis, second column is numeric.
        """

        options = options or {}
        go = Dependencies.get_module('plotly.graph_objs')
        self._build_chart_sub_tables(properties, x_type=self._get_plotly_chart_x_type(properties))
        if len(self.chart_sub_tables) < 1:
            return None
        n_colors = len(self.chart_sub_tables[0])

        # number of pies to display
        pies = len(self.chart_sub_tables)

        max_pies_in_row = 5
        pies_in_row = max_pies_in_row if pies >= max_pies_in_row else pies
        xinterval = 0 if pies_in_row < 2 else 0.05 / (pies_in_row - 1)
        xdelta = 1.0 if pies_in_row < 2 else 0.95 / pies_in_row
        # layout of pies
        number_of_rows = (pies + max_pies_in_row - 1) // max_pies_in_row
        yinterval = 0 if number_of_rows < 2 else 0.05 / (number_of_rows - 1)
        ydelta = 1.0 if number_of_rows < 2 else 0.95 / number_of_rows

        domains = [
            dict(
                x=[0 + (xdelta + xinterval) * (i % pies_in_row), 1 - (xdelta + xinterval) * (pies_in_row - (i % pies_in_row) - 1)],
                y=[0 + (ydelta + yinterval) * (i // pies_in_row), 1 - (ydelta + yinterval) * (number_of_rows - (i // pies_in_row) - 1)],
            )
            for i in range(0, pies)
        ]

        palette = self._get_palette(n_colors=n_colors)
        show_legend = properties.get(VisualizationKeys.LEGEND) != VisualizationLegends.HIDDEN
        title = properties.get(VisualizationKeys.TITLE) or VisualizationValues.PIE_CHART

        data = [
            go.Pie(
                labels=list(tab.keys()),
                values=list(tab.values()),
                domain=domains[idx],
                marker=dict(colors=palette),
                name=tab.name,
                textinfo="label+percent",
            )
            for idx, tab in enumerate(self.chart_sub_tables)
        ]
        layout = go.Layout(
            title=title,
            showlegend=show_legend,
            annotations=[
                dict(
                    text=tab.name,
                    showarrow=False,
                    x=(domains[idx].get("x")[0] + domains[idx].get("x")[1]) / 2,
                    y=domains[idx].get("y")[0] - 0.05 * ydelta,
                )
                for idx, tab in enumerate(self.chart_sub_tables)
            ],
        )
        return {"data": data, "layout": layout}


    def _render_barchart_plotly(self, properties:Dict[str,Any], key_word_sep:str=" ", options:Dict[str,Any]=None)->Dict[str,Any]:
        """Generates a pylab plot from the result set.

        Bar chart. 
        First column is x-axis, and can be text, datetime or numeric. Other columns are numeric, displayed as horizontal strips.
        """

        options = options or {}
        go = Dependencies.get_module('plotly.graph_objs')
        sub_tables = self._build_chart_sub_tables(properties, x_type=self._get_plotly_chart_x_type(properties))
        if len(sub_tables) < 1:
            # this print is not for debug
            print("No valid chart to show")
            return None
        chart_properties = self._get_plotly_chart_properties(properties, sub_tables, options=options)

        data = [
            go.Bar(
                x=list(tab.values()) if chart_properties.get("orientation") == "h" else list(tab.keys()),
                y=list(tab.keys()) if chart_properties.get("orientation") == "h" else list(tab.values()),
                marker=dict(color=self.get_color_from_palette(idx, n_colors=chart_properties.get("n_colors"))),
                name=tab.name,
                orientation=chart_properties.get("orientation"),
            )
            for idx, tab in enumerate(sub_tables)
        ]
        layout = go.Layout(
            title=chart_properties.get("title"),
            showlegend=chart_properties.get("showlegend"),
            xaxis=dict(
                title=chart_properties.get("xlabel"),
                type=chart_properties.get("xscale"),
                range=chart_properties.get("range") if chart_properties.get("orientation") == "h" else None,
                # dtick=20,
                ticksuffix="",
            ),
            yaxis=dict(
                type=chart_properties.get("yscale"),
                title=chart_properties.get("ylabel"),
                range=chart_properties.get("range") if chart_properties.get("orientation") != "h" else None,
            ),
        )
        return {"data": data, "layout": layout}


    def _render_linechart_plotly(self, properties:Dict[str,Any], key_word_sep:str=" ", options:Dict[str,Any]=None)->Dict[str,Any]:
        """Generates a pylab plot from the result set.

        Line graph. 
        First column is x-axis, and should be a numeric column. Other numeric columns are y-axes.
        """

        options = options or {}
        go = Dependencies.get_module('plotly.graph_objs')
        self._build_chart_sub_tables(properties, x_type=self._get_plotly_chart_x_type(properties))
        if len(self.chart_sub_tables) < 1:
            return None
        chart_properties = self._get_plotly_chart_properties(properties, self.chart_sub_tables, options=options)

        data = [
            go.Scatter(
                x=list(tab.keys()),
                y=list(tab.values()),
                name=tab.name,
                line=dict(width=1, color=self.get_color_from_palette(idx, n_colors=chart_properties.get("n_colors"))),
                opacity=0.8,
            )
            for idx, tab in enumerate(self.chart_sub_tables)
        ]
        layout = go.Layout(
            title=chart_properties.get("title"),
            showlegend=chart_properties.get("showlegend"),
            xaxis=dict(
                title=chart_properties.get("xlabel"),
                type=chart_properties.get("xscale"),
                autorange=chart_properties.get("autorange"),
            ),
            yaxis=dict(
                title=chart_properties.get("ylabel"),
                type=chart_properties.get("yscale"),
                range=chart_properties.get("range"),
                # dtick=20,
                # ticksuffix=''
            ),
        )
        return {"data": data, "layout": layout}


    def _render_scatterchart_plotly(self, properties:Dict[str,Any], key_word_sep:str=" ", options:Dict[str,Any]=None)->Dict[str,Any]:
        """Generates a pylab plot from the result set.

        Points graph. 
        First column is x-axis, and should be a numeric column. Other numeric columns are y-axes.
        """

        options = options or {}
        go = Dependencies.get_module('plotly.graph_objs')
        self._build_chart_sub_tables(properties, x_type=self._get_plotly_chart_x_type(properties))
        if len(self.chart_sub_tables) < 1:
            return None
        chart_properties = self._get_plotly_chart_properties(properties, self.chart_sub_tables, options=options)

        data = [
            go.Scatter(
                x=list(tab.keys()),
                y=list(tab.values()),
                name=tab.name,
                mode="markers",
                marker=dict(line=dict(width=1), color=self.get_color_from_palette(idx, n_colors=chart_properties.get("n_colors"))),
            )
            for idx, tab in enumerate(self.chart_sub_tables)
        ]
        layout = go.Layout(
            title=chart_properties.get("title"),
            showlegend=chart_properties.get("showlegend"),
            xaxis=dict(
                title=chart_properties.get("xlabel"),
                type=chart_properties.get("xscale"),
                autorange=chart_properties.get("autorange"),
            ),
            yaxis=dict(
                title=chart_properties.get("ylabel"),
                type=chart_properties.get("yscale"),
                range=chart_properties.get("range"),
                # dtick=20,
                ticksuffix="",
            ),
        )
        return {"data": data, "layout": layout}


prettytable = Dependencies.get_module('prettytable', dont_throw=True)
if prettytable:
    class PrettyTable(prettytable.PrettyTable):

        # Object constructor
        def __init__(self, *args, **kwargs):
            self.row_count = 0
            super(PrettyTable, self).__init__(*args, **kwargs)


        def add_rows(self, data):
            if self.row_count == len(data):
                return  # correct number of rows already present
            self.clear_rows()
            self.row_count = len(data)

            for row in data:
                r = [list(c) if isinstance(c, list) else dict(c) if isinstance(c, dict) else c for c in row]
                self.add_row(r)

# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

import copy
import functools
import operator
import csv
import codecs
import os.path
import re
import uuid
import base64


import six
import prettytable
from IPython.display import Image
import plotly
import plotly.graph_objs as go
try:
    import ipywidgets
except Exception:
    ipywidgets_installed = False
else:
    ipywidgets_installed = True


from .constants import VisualizationKeys, VisualizationValues, VisualizationScales, VisualizationLegends, VisualizationSplits, VisualizationKinds
from .my_utils import adjust_path
from .column_guesser import ColumnGuesserMixin
from .display import Display
from .palette import Palette, Palettes


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
        self.queue = six.StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()


    def writerow(self, row):
        if six.PY2:
            _row = [s.encode("utf-8") if hasattr(s, "encode") else s for s in row]
        else:
            _row = row
        self.writer.writerow(_row)
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        if six.PY2:
            data = data.decode("utf-8")
            # ... and reencode it into the target encoding
            data = self.encoder.encode(data)
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)
        self.queue.seek(0)


    def writerows(self, rows):
        for row in rows:
            self.writerow(row)


class FileResultDescriptor(bytes):
    """Provides IPython Notebook-friendly output for the feedback after a ``.csv`` called."""

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
            print(self._file_location_message())
            filename = adjust_path(self.file_or_image)
            return open(filename, "rb" if self.format in self.FILE_BINARY_FORMATS else "r").read()


    def _file_location_message(self):
        return "%s at %s" % (self.message, os.path.join(os.path.abspath("."), self.file_or_image))


    # Printable unambiguous presentation of the object
    def __repr__(self):
        if self.is_image:
            return "".join(chr(x) for x in self)
        elif self.show:
            return str(self._get_data())
        else:
            return self._file_location_message()


    # IPython html presentation of the object
    def _repr_html_(self):
        if self.show and self.format == "html":
            return self._get_data()
        if not self.show and not self.is_image:
            return '<a href="%s" download>%s</a>' % (os.path.join(".", "files", self.file_or_image), self.message)


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
    return "%s%s" % (match_obj.group(1), spaces)


class ResultSet(list, ColumnGuesserMixin):
    """
    Results of a query.

    Can access rows listwise, or by string value of leftmost column.
    """

    # Object constructor
    def __init__(self, queryResult, parametrized_query_dict, connection, fork_table_id, fork_table_resultSets, metadata, options):

        #         self.current_colors_palette = ['rgb(184, 247, 212)', 'rgb(111, 231, 219)', 'rgb(127, 166, 238)', 'rgb(131, 90, 241)']

        self.parametrized_query_dict = parametrized_query_dict
        self.fork_table_id = fork_table_id
        self._fork_table_resultSets = fork_table_resultSets
        self.options = options
        self.conn = connection
        # set by caller
        self.metadata = metadata
        self.feedback_info = []

        # table printing style to any of prettytable's defined styles (currently DEFAULT, MSWORD_FRIENDLY, PLAIN_COLUMNS, RANDOM)
        self.prettytable_style = prettytable.__dict__[self.options.get("prettytable_style", "DEFAULT").upper()]

        self.display_info = True
        self.suppress_result = False

        self._update(queryResult)


    def _get_palette(self, n_colors=None, desaturation=None):
        name = self.options.get("palette_name")
        length = max(n_colors or 10, self.options.get("palette_colors") or 10)
        self.metadata["palette"] = Palette(
            palette_name=name,
            n_colors=length,
            desaturation=desaturation or self.options.get("palette_desaturation"),
            to_reverse=self.options.get("palette_reverse"),
        )
        return self.palette


    def get_color_from_palette(self, idx, n_colors=None, desaturation=None):
        palette = self.palette or self._get_palette(n_colors, desaturation)
        if idx < len(palette):
            return str(palette[idx])
        return None


    @property 
    def parametrized_query(self):
        query_management_prefix: str = self.parametrized_query_dict.get('query_management_prefix')
        if (query_management_prefix and len(query_management_prefix) > 0):
            query_management_prefix += '\n'

        statements: list = self.parametrized_query_dict.get('statements')
        parametrized_query_str = query_management_prefix  + ";\n".join(statements)
        return parametrized_query_str


    @property
    def query(self):
        return self.metadata.get("parsed").get("query").strip()


    @property
    def plotly_fig(self):
        return self.metadata.get("figure_or_data")


    @property
    def palette(self):
        return self.metadata.get("palette")


    @property
    def palettes(self):
        return Palettes(n_colors=self.options.get("palette_colors"), desaturation=self.options.get("palette_desaturation"))


    @property
    def connection(self):
        return self.metadata.get("connection")


    @property
    def start_time(self):
        return self.metadata.get("start_time")


    @property
    def end_time(self):
        return self.metadata.get("end_time")


    @property
    def elapsed_timespan(self):
        return self.end_time - self.start_time


    @property
    def visualization(self):
        return self.visualization_properties.get(VisualizationKeys.VISUALIZATION)


    @property
    def title(self):
        return self.visualization_properties.get(VisualizationKeys.TITLE)


    def deep_link(self, qld_param: str=None):
        if (qld_param and qld_param not in ["Kusto.Explorer", "Kusto.WebExplorer"]):
            raise ValueError('Unknow deep link destination, the only supported are: ["Kusto.Explorer", "Kusto.WebExplorer"]')
        options = {**self.options, "query_link_destination": qld_param } if qld_param else self.options
        deep_link_url = self.conn.get_deep_link(self.parametrized_query, options)
        if deep_link_url is not None: #only use deep links for kusto connection
            qld = options.get("query_link_destination").lower().replace('.', '_')
            isCloseWindow = options.get("query_link_destination") == "Kusto.Explorer"
            html_obj = Display.get_show_deeplink_html_obj(f"query_link_{qld}", deep_link_url, isCloseWindow, **self.options)
            return html_obj
        else:
            raise ValueError('Deep link not supported for this connection, only Azure Data Explorer connections are supported')
        return None


    def _update(self, queryResult):
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
        self.pretty = PrettyTable(self.field_names, style=self.prettytable_style) if len(self.field_names) > 0 else None
        self.records_count = queryResultTable.recordscount()
        self.is_partial_table = queryResultTable.ispartial()
        self.visualization_properties = queryResultTable.visualization_properties
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
    

    def _create_fork_results(self):
        if self.fork_table_id == 0 and len(self._fork_table_resultSets) == 1:
            for fork_table_id in range(1, len(self._queryResult.tables)):
                r = ResultSet(self._queryResult, self.parametrized_query_dict, self.conn, fork_table_id, self._fork_table_resultSets, self.metadata, self.options)
                if r.options.get("feedback"):
                    if r.options.get("show_query_time"):
                        minutes, seconds = divmod(self.elapsed_timespan, 60)
                        r.feedback_info.append("Done ({:0>2}:{:06.3f}): {} records".format(int(minutes), seconds, r.records_count))


    def _update_fork_results(self):
        if self.fork_table_id == 0:
            for r in self._fork_table_resultSets.values():
                if r != self:
                    r._update(self._queryResult)
                    r.metadata = self.metadata
                    r.display_info = True
                    r.suppress_result = False
                    r.feedback_info = []
                    if r.options.get("feedback"):
                        if r.options.get("show_query_time"):
                            minutes, seconds = divmod(self.elapsed_timespan, 60)
                            r.feedback_info.append("Done ({:0>2}:{:06.3f}): {} records".format(int(minutes), seconds, r.records_count))


    def fork_result(self, fork_table_id=0):
        # return self._fork_table_resultSets.get(str(fork_table_id))
        return self._fork_table_resultSets[str(fork_table_id)]


    @property
    def raw_json(self):
        return Display.to_styled_class(self._json_response, **self.options)


    @property
    def completion_query_info(self):
        return Display.to_styled_class(self._completion_query_info, **self.options)


    @property
    def completion_query_resource_consumption(self):
        return Display.to_styled_class(self._completion_query_resource_consumption, **self.options)


    @property
    def dataSetCompletion(self):
        return Display.to_styled_class(self._dataSetCompletion, **self.options)


    # IPython html presentation of the object
    def _repr_html_(self):
        if not self.suppress_result:
            if self.display_info:
                Display.showInfoMessage(self.metadata.get("conn_info"))
                if self.options.get("show_query"):
                    Display.showInfoMessage(self.parametrized_query)

            if self.is_chart():
                self.show_chart(**self.options)
            else:
                self.show_table(**self.options)
            
            if self.display_info:
                Display.showInfoMessage(self.feedback_info)

                if self.options.get("show_query_link"):
                    self.show_button_to_deep_link()

        # display info only once
        self.display_info = False

        # suppress results info only once
        self.suppress_result = False
        return ""


    # use _.open_url_kusto_explorer(True) for opening the url automatically (no button)
    # use _.open_url_kusto_explorer(web_app="app") for opening the url in Kusto Explorer (app) and not in Kusto Web Explorer
    def show_button_to_deep_link(self, browser=False):
        deep_link_url = self.conn.get_deep_link(self.parametrized_query, self.options)
        if deep_link_url is not None: #only use deep links for kusto connection 
            qld = self.options.get("query_link_destination").lower().replace('.', '_')
            Display.show_window(
                f"query_link_{qld}", 
                deep_link_url, 
                f"{self.options.get('query_link_destination')}", 
                onclick_visibility="visible",
                palette=Display.info_style,
                before_text=f"Click to execute query in {self.options.get('query_link_destination')} "
            )
        return None


    def _getTableHtml(self):
        "get query result in a table format as an HTML string"
        _cell_with_spaces_pattern = re.compile(r"(<td>)( {2,})")
        if self.pretty:
            self.pretty.add_rows(self)
            result = self.pretty.get_html_string()
            result = _cell_with_spaces_pattern.sub(_nonbreaking_spaces, result)
            display_limit = 0 if not self.options.get("display_limit") else self.options.get("display_limit")
            if display_limit > 0 and len(self) > display_limit:
                result = '%s\n<span style="font-style:italic;text-align:center;">%d rows, truncated to display_limit of %d</span>' % (
                    result,
                    len(self),
                    display_limit,
                )
            return {"body": result}
        else:
            return {}


    def show_table(self, **kwargs):
        "display the table"
        options = {**self.options, **kwargs}
        if len(self) == 1 and len(self[0]) == 1  and (isinstance(self[0][0], dict) or isinstance(self[0][0], list)):
            Display.show(Display.to_styled_class(self[0][0]), **options)
            return None
        elif options.get("table_package", "").upper() == "PANDAS":
            t = self.to_dataframe()._repr_html_()
            html = Display.toHtml(body=t)
        else:
            t = self._getTableHtml()
            html = Display.toHtml(**t)
        if options.get("popup_window") and not options.get("button_text"):
            options["button_text"] = "popup " + "table" + ((" - " + self.title) if self.title else "") + " "
        Display.show(html, **options)
        return None


    def popup_table(self, **kwargs):
        "display the table in popup window"
        return self.show_table(**{"popup_window": True, **kwargs})


    def display_table(self, **kwargs):
        "display the table in cell"
        return self.show_table(**{"popup_window": False, **kwargs})


    # Printable pretty presentation of the object
    def __str__(self, *args, **kwargs):
        self.pretty.add_rows(self)
        return str(self.pretty or "")


    # For iterator self[key]
    def __getitem__(self, key):
        """
        Access by integer (row position within result set)
        or by string (value of leftmost column)
        """
        try:
            return list.__getitem__(self, key)
        except TypeError:
            result = [row for row in self if row[0] == key]
            if not result or len(result) == 0:
                raise KeyError(key)
            if len(result) > 1:
                raise KeyError('%d results for "%s"' % (len(result), key))
            return result[0]


    def to_dict(self):
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


    def to_dataframe(self):
        "Returns a Pandas DataFrame instance built from the result set."
        if self._dataframe is None:
            self._dataframe = self._queryResult.tables[self.fork_table_id].to_dataframe()

            # import pandas as pd
            # frame = pd.DataFrame(self, columns=(self and self.columns_name) or [])
            # self._dataframe = frame
        return self._dataframe


    def submit(self):
        "execute the query again"
        magic = self.metadata.get("magic")
        line = self.metadata.get("parsed").get("line")
        cell = self.metadata.get("parsed").get("cell")
        return magic.execute(line, cell)


    def refresh(self):
        "refresh the results of the query"
        magic = self.metadata.get("magic")
        user_ns = magic.shell.user_ns.copy()
        return magic.execute_query(self.metadata.get("parsed"), user_ns, self)


    def show_chart(self, **kwargs):
        "display the chart that was specified in the query"
        options = {**self.options, **kwargs}
        window_mode = options is not None and options.get("popup_window")

        if window_mode and not options.get("button_text"):
            options["button_text"] = "popup " + self.visualization + ((" - " + self.title) if self.title else "") + " "
        c = self._getChartHtml(window_mode, **options)
        if c.get("body") or c.get("head"):
            html = Display.toHtml(**c)
            Display.show(html, **options)
        elif c.get("fig"):
            if Display.notebooks_host or options.get("notebook_app") in ["jupyterlab", "visualstudiocode", "ipython"]: 
                plotly.offline.init_notebook_mode(connected=True)
                plotly.offline.iplot(c.get("fig"), filename="plotlychart")
            else:
                Display.show(c.get("fig"), **options)
        else:
            return self.show_table(**kwargs)


    def to_image(self, **params):
        "export image of the chart that was specified in the query to a file"
        _options = {**self.options, **params}

        if self.options.get("plot_package") == "plotly_orca" or self.options.get("plot_package") == "plotly":

            _options = {**self.options, **{"plot_package": "plotly"}}
            fig = self._getChartHtml(window_mode=False, **_options).get("fig")
            if fig is not None:

                filename = adjust_path(params.get("filename"))
                file_or_image_bytes = self._plotly_fig_to_image(fig, filename, **params)

                if file_or_image_bytes:

                    return FileResultDescriptor(file_or_image_bytes, message="image results", format=params.get("format"), show=params.get("show"))


    def popup_Chart(self, **kwargs):
        "display the chart that was specified in the query in a popup window"
        return self.popup(**kwargs)


    def display_Chart(self, **kwargs):
        "display the chart that was specified in the query in the cell"
        return self.display(**kwargs)


    def popup(self, **kwargs):
        "display the chart that was specified in the query"
        return self.show_chart(**{"popup_window": True, **kwargs})


    def display(self, **kwargs):
        "display the chart that was specified in the query"
        return self.show_chart(**{"popup_window": False, **kwargs})


    def is_chart(self):
        return self.visualization and self.visualization != VisualizationValues.TABLE


    _SUPPORTED_PLOT_PACKAGES = [
        "plotly",
        "plotly_orca"
    ]


    def _getChartHtml(self, window_mode=False, **options):
        "get query result in a char format as an HTML string"
        # https://kusto.azurewebsites.net/docs/queryLanguage/query_language_renderoperator.html

        if not self.is_chart():
            return {}

        if options.get("plot_package") == "None":
            return {}

        if options.get("plot_package") not in self._SUPPORTED_PLOT_PACKAGES:
            return {}

        if len(self) == 0:
            id = uuid.uuid4().hex
            head = (
                """<style>#uuid-"""
                + id
                + """ {
                display: block; 
                font-style:italic;
                font-size:300%;
                text-align:center;
            } </style>"""
            )

            body = '<div id="uuid-' + id + '"><br><br>EMPTY CHART (no data)<br><br>.</div>'
            return {"body": body, "head": head}

        figure_or_data = None

        # First column is color-axis, second column is numeric
        if self.visualization == VisualizationValues.PIE_CHART:
            figure_or_data = self._render_piechart_plotly(self.visualization_properties, " ")
            # chart = self._render_pie(self.visualization_properties, " ")

        # First column is x-axis, and can be text, datetime or numeric. Other columns are numeric, displayed as horizontal strips.
        # kind = default, unstacked, stacked, stacked100 (Default, same as unstacked; unstacked - Each "area" to its own; stacked - "Areas" are stacked to the right; stacked100 - "Areas" are stacked to the right, and stretched to the same width)
        elif self.visualization == VisualizationValues.BAR_CHART:
            figure_or_data = self._render_barchart_plotly(self.visualization_properties, " ")
            # chart = self._render_barh(self.visualization_properties, " ")

        # Like barchart, with vertical strips instead of horizontal strips.
        # kind = default, unstacked, stacked, stacked100
        elif self.visualization == VisualizationValues.COLUMN_CHART:
            figure_or_data = self._render_barchart_plotly(self.visualization_properties, " ")
            # chart = self._render_bar(self.visualization_properties, " ")

        # Area graph. First column is x-axis, and should be a numeric column. Other numeric columns are y-axes.
        # kind = default, unstacked, stacked, stacked100
        elif self.visualization == VisualizationValues.AREA_CHART:
            figure_or_data = self._render_areachart_plotly(self.visualization_properties, " ")
            # chart = self._render_areachart(self.visualization_properties, " ")

        # Line graph. First column is x-axis, and should be a numeric column. Other numeric columns are y-axes.
        elif self.visualization == VisualizationValues.LINE_CHART:
            figure_or_data = self._render_linechart_plotly(self.visualization_properties, " ")

        # Line graph. First column is x-axis, and should be datetime. Other columns are y-axes.
        elif self.visualization == VisualizationValues.TIME_CHART:
            figure_or_data = self._render_timechart_plotly(self.visualization_properties, " ")

        # Similar to timechart, but highlights anomalies using an external machine-learning service.
        elif self.visualization == VisualizationValues.ANOMALY_CHART:
            figure_or_data = self._render_linechart_plotly(self.visualization_properties, " ")

        # Stacked area graph. First column is x-axis, and should be a numeric column. Other numeric columns are y-axes.
        elif self.visualization == VisualizationValues.STACKED_AREA_CHART:
            figure_or_data = self._render_stackedareachart_plotly(self.visualization_properties, " ")

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
            figure_or_data = self._render_scatterchart_plotly(self.visualization_properties, " ")

        if figure_or_data is not None:
            self.metadata["figure_or_data"] = figure_or_data
            if options.get("plot_package") == "plotly_orca":
                image_bytes = self._plotly_fig_to_image(figure_or_data, None, **options)
                image_base64_bytes= base64.b64encode(image_bytes)
                image_base64_str = image_base64_bytes.decode("utf-8")
                image_html_str = f"""<div><img src='data:image/png;base64,{image_base64_str}'></div>"""
                return {"body": image_html_str}
                
            elif window_mode:
                head = '<script src="https://cdn.plot.ly/plotly-latest.min.js"></script>' if not self.options.get("plotly_fs_includejs") else None
                body = plotly.offline.plot(
                    figure_or_data,
                    include_plotlyjs=window_mode and self.options.get("plotly_fs_includejs", False), 
                    output_type="div"
                )
                return {"body": body, "head": head}

            else:
                return {"fig": figure_or_data}
        return {}


    def _plotly_fig_to_image(self, fig, filename, **kwargs):
        params = kwargs or {}
        try:
            if filename: #requires plotly orca package

                fig.write_image(
                    adjust_path(filename),
                    format=params.get("format"), 
                    scale=params.get("scale"), width=params.get("width"), height=params.get("height")
                )
                # plotly.io.write_image(
                #     fig, file, format=params.get("format"), scale=params.get("scale"), width=params.get("width"), height=params.get("height")
                # )
                return filename        
            else:
                return plotly.io.to_image(
                    fig, format=params.get("format"), scale=params.get("scale"), width=params.get("width"), height=params.get("height")
                )
        except:
            return None


    def pie(self, properties:dict, key_word_sep=" ", **kwargs):
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

        self.build_columns()
        import matplotlib.pyplot as plt

        pie = plt.pie(self.columns[1], labels=self.columns[0], **kwargs)
        plt.title(properties.get(VisualizationKeys.TITLE) or self.columns[1].name)
        plt.show()
        return pie


    def plot(self, properties:dict, **kwargs):
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
        import matplotlib.pylab as plt

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


    def bar(self, properties:dict, key_word_sep=" ", **kwargs):
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
        import matplotlib.pylab as plt

        self.guess_pie_columns(xlabel_sep=key_word_sep)
        plot = plt.bar(range(len(self.ys[0])), self.ys[0], **kwargs)
        if self.xlabels:
            plt.xticks(range(len(self.xlabels)), self.xlabels, rotation=45)
        plt.xlabel(self.xlabel)
        plt.ylabel(self.ys[0].name)
        return plot


    def to_csv(self, filename=None, **kwargs):
        """Generate results in comma-separated form.  Write to ``filename`` if given.
           Any other parameters will be passed on to csv.writer."""
        if not self.pretty:
            return None  # no results
        self.pretty.add_rows(self)
        if filename:
            filename = adjust_path(filename)
            encoding = kwargs.get("encoding", "utf-8")
            if six.PY2:
                outfile = open(filename, "wb")
            else:
                outfile = open(filename, "w", newline="", encoding=encoding)
        else:
            outfile = six.StringIO()
        writer = UnicodeWriter(outfile, **kwargs)
        writer.writerow(self.field_names)
        for row in self:
            writer.writerow(row)
        if filename:
            outfile.close()
            message = "csv results"
            return FileResultDescriptor(filename, message=message, format="csv", **kwargs)
        else:
            return outfile.getvalue()


    def _render_pie(self, properties:dict, key_word_sep=" ", **kwargs):
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
        self.build_columns()
        import matplotlib.pylab as plt

        pie = plt.pie(self.columns[1], labels=self.columns[0], **kwargs)
        plt.title(properties.get(VisualizationKeys.TITLE) or self.columns[1].name)
        plt.show()
        return pie


    def _render_barh(self, properties:dict, key_word_sep=" ", **kwargs):
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
        import matplotlib.pylab as plt

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


    def _render_bar(self,properties:dict, key_word_sep=" ", **kwargs):
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
        import matplotlib.pylab as plt

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


    def _render_linechart(self, properties:dict, key_word_sep=" ", **kwargs):
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

        import matplotlib.pyplot as plt

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


    def _get_plotly_axis_scale(self, specified_property: str, col=None):
        if col is not None:
            if not col.is_quantity:
                return "category"
            elif col.is_datetime:
                return "date"
        return "log" if specified_property == VisualizationScales.LOG else "linear"


    def _get_plotly_ylabel(self, specified_property: str, tabs: list) -> str:
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


    def _get_plotly_chart_x_type(self, properties: dict) -> str:
        return self._CHART_X_TYPE.get(properties.get(VisualizationKeys.VISUALIZATION), "first")
    

    def _get_plotly_chart_properties(self, properties: dict, tabs: list) -> dict:
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
        return chart_properties


    def _figure_or_figurewidget(self, data, layout):
        if ipywidgets_installed:
            # print("----------- FigureWidget --------------")
            fig = go.FigureWidget(data=data, layout=layout)
        else:
            # print("----------- Figure --------------")
            fig = go.Figure(data=data, layout=layout)
        return fig


    def _render_areachart_plotly(self, properties: dict, key_word_sep=" ", **kwargs):
        """Generates a pylab plot from the result set.

        Area graph. 
        First column is x-axis, and should be a numeric column. Other numeric columns are y-axes.
        """

        self._build_chart_sub_tables(properties , x_type=self._get_plotly_chart_x_type(properties))
        if len(self.chart_sub_tables) < 1:
            return None
        chart_properties = self._get_plotly_chart_properties(properties, self.chart_sub_tables)

        data = [
            go.Scatter(
                x=list(tab.keys()),
                y=list(tab.values()),
                name=tab.name,
                mode="lines",
                line=dict(width=0.5, color=self.get_color_from_palette(idx, n_colors=chart_properties["n_colors"])),
                fill="tozeroy",
            )
            for idx, tab in enumerate(self.chart_sub_tables)
        ]
        layout = go.Layout(
            title=chart_properties["title"],
            showlegend=chart_properties["showlegend"],
            xaxis=dict(
                title=chart_properties["xlabel"], 
                type=chart_properties["xscale"],
                autorange=chart_properties["autorange"],
            ),
            yaxis=dict(
                title=chart_properties["ylabel"],
                type=chart_properties["yscale"],
                # range=[0, 3],
                # dtick=20,
                ticksuffix="",
            ),
        )
        fig = self._figure_or_figurewidget(data=data, layout=layout)
        return fig


    def _render_stackedareachart_plotly(self, properties:dict, key_word_sep=" ", **kwargs):
        """Generates a pylab plot from the result set.

        Stacked area graph. 
        First column is x-axis, and should be a numeric column. Other numeric columns are y-axes.
        """

        self._build_chart_sub_tables(properties, x_type=self._get_plotly_chart_x_type(properties))
        if len(self.chart_sub_tables) < 1:
            return None
        chart_properties = self._get_plotly_chart_properties(properties, self.chart_sub_tables)

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
                line=dict(width=0.5, color=self.get_color_from_palette(idx, n_colors=chart_properties["n_colors"])),
                fill="tonexty",
            )
            for idx, tab in enumerate(self.chart_sub_tables)
        ]
        layout = go.Layout(
            title=chart_properties["title"],
            showlegend=chart_properties["showlegend"],
            xaxis=dict(
                title=chart_properties["xlabel"], 
                type=chart_properties["xscale"],
                autorange=chart_properties["autorange"],
            ),
            yaxis=dict(
                title=chart_properties["ylabel"],
                type=chart_properties["yscale"],
                # range=[0, 3],
                # dtick=20,
                ticksuffix="",
            ),
        )
        fig = self._figure_or_figurewidget(data=data, layout=layout)
        return fig


    def _render_timechart_plotly(self, properties: dict, key_word_sep=" ", **kwargs):
        """Generates a pylab plot from the result set.

        Line graph. 
        First column is x-axis, and should be datetime. Other columns are y-axes.
        """

        self._build_chart_sub_tables(properties, x_type=self._get_plotly_chart_x_type(properties))
        if len(self.chart_sub_tables) < 1:
            return None
        chart_properties = self._get_plotly_chart_properties(properties, self.chart_sub_tables)

        data = [
            go.Scatter(
                x=list(tab.keys()),
                y=list(tab.values()),
                name=tab.name,
                line=dict(width=1, color=self.get_color_from_palette(idx, n_colors=chart_properties["n_colors"])),
                opacity=0.8,
            )
            for idx, tab in enumerate(self.chart_sub_tables)
        ]

        layout = go.Layout(
            title=chart_properties["title"],
            showlegend=chart_properties["showlegend"],
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
                title=chart_properties["xlabel"],
                type=chart_properties["xscale"],
                autorange=chart_properties["autorange"],
            ),
            yaxis=dict(
                title=chart_properties["ylabel"],
                type=chart_properties["yscale"],
                # range=[0, 3],
                # dtick=20,
                ticksuffix="",
            ),
        )
        fig = self._figure_or_figurewidget(data=data, layout=layout)
        return fig


    def _render_piechart_plotly(self, properties: dict, key_word_sep=" ", **kwargs):
        """Generates a pylab plot from the result set.

        Pie chart. 
        First column is color-axis, second column is numeric.
        """

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
        fig = self._figure_or_figurewidget(data=data, layout=layout)
        return fig


    def _render_barchart_plotly(self, properties: dict, key_word_sep=" ", **kwargs):
        """Generates a pylab plot from the result set.

        Bar chart. 
        First column is x-axis, and can be text, datetime or numeric. Other columns are numeric, displayed as horizontal strips.
        """

        sub_tables = self._build_chart_sub_tables(properties, x_type=self._get_plotly_chart_x_type(properties))
        if len(sub_tables) < 1:
            print("No valid chart to show")
            return None
        chart_properties = self._get_plotly_chart_properties(properties, sub_tables)

        data = [
            go.Bar(
                x=list(tab.values()) if chart_properties["orientation"] == "h" else list(tab.keys()),
                y=list(tab.keys()) if chart_properties["orientation"] == "h" else list(tab.values()),
                marker=dict(color=self.get_color_from_palette(idx, n_colors=chart_properties["n_colors"])),
                name=tab.name,
                orientation=chart_properties["orientation"],
            )
            for idx, tab in enumerate(sub_tables)
        ]
        layout = go.Layout(
            title=chart_properties["title"],
            showlegend=chart_properties["showlegend"],
            xaxis=dict(
                title=chart_properties["xlabel"],
                type=chart_properties["xscale"],
                # range=[0, 3],
                # dtick=20,
                ticksuffix="",
            ),
            yaxis=dict(
                type=chart_properties["yscale"],
                title=chart_properties["ylabel"],
            ),
        )
        fig = self._figure_or_figurewidget(data=data, layout=layout)
        return fig


    def _render_linechart_plotly(self, properties: dict, key_word_sep=" ", **kwargs):
        """Generates a pylab plot from the result set.

        Line graph. 
        First column is x-axis, and should be a numeric column. Other numeric columns are y-axes.
        """

        self._build_chart_sub_tables(properties, x_type=self._get_plotly_chart_x_type(properties))
        if len(self.chart_sub_tables) < 1:
            return None
        chart_properties = self._get_plotly_chart_properties(properties, self.chart_sub_tables)

        data = [
            go.Scatter(
                x=list(tab.keys()),
                y=list(tab.values()),
                name=tab.name,
                line=dict(width=1, color=self.get_color_from_palette(idx, n_colors=chart_properties["n_colors"])),
                opacity=0.8,
            )
            for idx, tab in enumerate(self.chart_sub_tables)
        ]
        layout = go.Layout(
            title=chart_properties["title"],
            showlegend=chart_properties["showlegend"],
            xaxis=dict(
                title=chart_properties["xlabel"],
                type=chart_properties["xscale"],
                autorange=chart_properties["autorange"],
            ),
            yaxis=dict(
                title=chart_properties["ylabel"],
                type=chart_properties["yscale"],
                # range=[0, 3],
                # dtick=20,
                # ticksuffix=''
            ),
        )
        fig = self._figure_or_figurewidget(data=data, layout=layout)
        return fig


    def _render_scatterchart_plotly(self, properties: dict, key_word_sep=" ", **kwargs):
        """Generates a pylab plot from the result set.

        Points graph. 
        First column is x-axis, and should be a numeric column. Other numeric columns are y-axes.
        """

        self._build_chart_sub_tables(properties, x_type=self._get_plotly_chart_x_type(properties))
        if len(self.chart_sub_tables) < 1:
            return None
        chart_properties = self._get_plotly_chart_properties(properties, self.chart_sub_tables)

        data = [
            go.Scatter(
                x=list(tab.keys()),
                y=list(tab.values()),
                name=tab.name,
                mode="markers",
                marker=dict(line=dict(width=1), color=self.get_color_from_palette(idx, n_colors=chart_properties["n_colors"])),
            )
            for idx, tab in enumerate(self.chart_sub_tables)
        ]
        layout = go.Layout(
            title=chart_properties["title"],
            showlegend=chart_properties["showlegend"],
            xaxis=dict(
                title=chart_properties["xlabel"], 
                type=chart_properties["xscale"],
                autorange=chart_properties["autorange"],
            ),
            yaxis=dict(
                title=chart_properties["ylabel"],
                type=chart_properties["yscale"],
                # range=[0, 3],
                # dtick=20,
                ticksuffix="",
            ),
        )
        fig = self._figure_or_figurewidget(data=data, layout=layout)
        return fig


class PrettyTable(prettytable.PrettyTable):

    # Object constructor
    def __init__(self, *args, **kwargs):
        self.row_count = 0
        self.display_limit = None
        super(PrettyTable, self).__init__(*args, **kwargs)


    def add_rows(self, data):
        if self.row_count and (data.options.get("display_limit") == self.display_limit):
            return  # correct number of rows already present
        self.clear_rows()
        self.display_limit = data.options.get("display_limit")
        if self.display_limit == 0:
            self.display_limit = None  # TODO: remove this to make 0 really 0
        if self.display_limit in (None, 0):
            self.row_count = len(data)
        else:
            self.row_count = min(len(data), self.display_limit)

        for row in data[: self.display_limit]:
            r = [list(c) if isinstance(c, list) else dict(c) if isinstance(c, dict) else c for c in row]
            self.add_row(r)


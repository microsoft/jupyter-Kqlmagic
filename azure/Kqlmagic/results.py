#-------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
#--------------------------------------------------------------------------

import copy
import functools
import operator
import csv
import six
import codecs
import os.path
import re
import uuid
import prettytable
from Kqlmagic.column_guesser import ColumnGuesserMixin

from Kqlmagic.display import Display

from Kqlmagic.palette import Palette, Palettes

import plotly

import plotly.plotly as py
import plotly.graph_objs as go


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

    FILE_BINARY_FORMATS = ['png', 'pdf', 'jpeg', 'jpg', 'eps']
    FILE_STRING_FORMATS = ['svg', 'webp', 'csv']

    @staticmethod
    def get_format(file, format=None):
        if format is None and file is not None and isinstance(file, str):
            parts = file.split('.')
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
            return open(self.file_or_image, 'rb' if self.format in self.FILE_BINARY_FORMATS else 'r').read()

    def _file_location_message(self):
        return "%s at %s" % (self.message, os.path.join(os.path.abspath("."), self.file_or_image))

    # Printable unambiguous presentation of the object
    def __repr__(self):
        if self.is_image:
            return "".join( chr(x) for x in self)
        elif self.show:
            return str(self._get_data())
        else:
            return self._file_location_message()

    # IPython html presentation of the object
    def _repr_html_(self):
        if self.show and self.format == 'html':
            return self._get_data()
        if not self.show and not self.is_image:
            return '<a href="%s" download>%s</a>' % (os.path.join(".", "files", self.file_or_image), self.message)

    def _repr_png_(self):
        if self.show and self.format == 'png':
            print('_repr_png_')
            return self._get_data()

    def _repr_jpeg_(self):
        if self.show and (self.format == 'jpeg' or self.format == 'jpg'):
            return self._get_data()

    def _repr_svg_(self):
        if self.show and self.format == 'svg':
            return self._get_data()

    def _repr_webp_(self):
        if self.show and self.format == 'webp':
            return self._get_data()

    def _repr_pdf_(self):
        if self.show and self.format == 'pdf':
            return self._get_data()

    def _repr_eps_(self):
        if self.show and self.format == 'eps':
            return self._get_data()


def _nonbreaking_spaces(match_obj):
    """
    Make spaces visible in HTML by replacing all `` `` with ``&nbsp;``

    Call with a ``re`` match object.  Retain group 1, replace group 2
    with nonbreaking speaces.
    """
    spaces = "&nbsp;" * len(match_obj.group(2))
    return "%s%s" % (match_obj.group(1), spaces)


_cell_with_spaces_pattern = re.compile(r"(<td>)( {2,})")


class ResultSet(list, ColumnGuesserMixin):
    """
    Results of a query.

    Can access rows listwise, or by string value of leftmost column.
    """

    # Object constructor
    def __init__(self, queryResult, parametrized_query, fork_table_id, fork_table_resultSets, metadata, options):

        #         self.current_colors_palette = ['rgb(184, 247, 212)', 'rgb(111, 231, 219)', 'rgb(127, 166, 238)', 'rgb(131, 90, 241)']

        self.parametrized_query = parametrized_query
        self.fork_table_id = fork_table_id
        self._fork_table_resultSets = fork_table_resultSets
        self.options = options

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

    def _update(self, queryResult):
        self._queryResult = queryResult
        self._completion_query_info = queryResult.completion_query_info
        self._completion_query_resource_consumption = queryResult.completion_query_resource_consumption
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
        self.visualization = queryResultTable.visualization_property("Visualization")
        self.title = queryResultTable.visualization_property("Title")
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
                r = ResultSet(self._queryResult, self.parametrized_query, fork_table_id, self._fork_table_resultSets, self.metadata, self.options)
                if r.options.get("feedback"):
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
                        minutes, seconds = divmod(self.elapsed_timespan, 60)
                        r.feedback_info.append("Done ({:0>2}:{:06.3f}): {} records".format(int(minutes), seconds, r.records_count))

    def fork_result(self, fork_table_id=0):
        #return self._fork_table_resultSets.get(str(fork_table_id))
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

    # IPython html presentation of the object
    def _repr_html_(self):
        if not self.suppress_result:
            if self.display_info:
                Display.showInfoMessage(self.metadata.get("conn_info"))

            if self.is_chart():
                self.show_chart(**self.options)
            else:
                self.show_table(**self.options)

            if self.display_info:
                Display.showInfoMessage(self.feedback_info)

        # display info only once
        self.display_info = False

        # suppress results info only once
        self.suppress_result = False
        return ""

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
        if options.get("table_package", "").upper() == "PANDAS":
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
        "display the chart that was specified in the query"
        magic = self.metadata.get("magic")
        user_ns = magic.shell.user_ns.copy()
        return magic.execute_query(self.metadata.get("parsed"), user_ns)

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
        c = self._getChartHtml(window_mode)
        if c.get("body") or c.get("head"):
            html = Display.toHtml(**c)
            Display.show(html, **options)
        elif c.get("fig"):
            if Display.notebooks_host or options.get("notebook_app") == "jupyterlab":
                plotly.offline.init_notebook_mode(connected=True)
                plotly.offline.iplot(c.get("fig"), filename="plotlychart")
            else:
                Display.show(c.get("fig"), **options)
        else:
            return self.show_table(**kwargs)


    def to_image(self, **kwargs):
        "export image of the chart that was specified in the query to a file"
        params = kwargs or {}
        fig = self._getChartHtml().get("fig")
        if fig is not None:
            file = params.get('filename')
            image = self._export_chart_image_plotly(fig, file, **kwargs)
            return FileResultDescriptor(image,message='image results', format=params.get('format'), show=params.get('show') )


    def _export_chart_image_plotly(self, fig, file, **kwargs):
        params = kwargs or {}
        if file:
            plotly.io.write_image(fig, file, format=params.get('format'),
                          scale=params.get('scale'), width=params.get('width'), height=params.get('height'))
            return file
        else:
            return plotly.io.to_image(fig, format=params.get('format'),
                          scale=params.get('scale'), width=params.get('width'), height=params.get('height'))

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
        return self.visualization and not self.visualization == "table"

    def _getChartHtml(self, window_mode=False):
        "get query result in a char format as an HTML string"
        # https://kusto.azurewebsites.net/docs/queryLanguage/query_language_renderoperator.html

        if not self.is_chart():
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
        if self.visualization == "piechart":
            figure_or_data = self._render_piechart_plotly(" ", self.title)
            # chart = self._render_pie(" ", self.title)
        # First column is x-axis, and can be text, datetime or numeric. Other columns are numeric, displayed as horizontal strips.
        # kind = default, unstacked, stacked, stacked100 (Default, same as unstacked; unstacked - Each "area" to its own; stacked - "Areas" are stacked to the right; stacked100 - "Areas" are stacked to the right, and stretched to the same width)
        elif self.visualization == "barchart":
            figure_or_data = self._render_barchart_plotly(" ", self.title)
            # chart = self._render_barh(" ", self.title)
        # Like barchart, with vertical strips instead of horizontal strips.
        # kind = default, unstacked, stacked, stacked100
        elif self.visualization == "columnchart":
            figure_or_data = self._render_columnchart_plotly(" ", self.title)
            # chart = self._render_bar(" ", self.title)
        # Area graph. First column is x-axis, and should be a numeric column. Other numeric columns are y-axes.
        # kind = default, unstacked, stacked, stacked100
        elif self.visualization == "areachart":
            figure_or_data = self._render_areachart_plotly(" ", self.title)
            # chart = self._render_areachart(" ", self.title)
        # Line graph. First column is x-axis, and should be a numeric column. Other numeric columns are y-axes.
        elif self.visualization == "linechart":
            figure_or_data = self._render_linechart_plotly(" ", self.title)
        # Line graph. First column is x-axis, and should be datetime. Other columns are y-axes.
        elif self.visualization == "timechart":
            figure_or_data = self._render_timechart_plotly(" ", self.title)
        # Similar to timechart, but highlights anomalies using an external machine-learning service.
        elif self.visualization == "anomalychart":
            figure_or_data = self._render_anomalychart_plotly(" ", self.title)
        # Stacked area graph. First column is x-axis, and should be a numeric column. Other numeric columns are y-axes.
        elif self.visualization == "stackedareachart":
            figure_or_data = self._render_stackedareachart_plotly(" ", self.title)
        # Last two columns are the x-axis, other columns are y-axis.
        elif self.visualization == "ladderchart":
            figure_or_data = self.pie(" ", self.title)
        # Interactive navigation over the events time-line (pivoting on time axis)
        elif self.visualization == "timepivot":
            figure_or_data = self.pie(" ", self.title)
        # Displays a pivot table and chart. User can interactively select data, columns, rows and various chart types.
        elif self.visualization == "pivotchart":
            figure_or_data = self.pie(" ", self.title)
        # Points graph. First column is x-axis, and should be a numeric column. Other numeric columns are y-axes
        elif self.visualization == "scatterchart":
            figure_or_data = self._render_scatterchart_plotly(" ", self.title)

        if figure_or_data is not None:
            self.metadata["figure_or_data"] = figure_or_data
            if window_mode:
                head = (
                    '<script src="https://cdn.plot.ly/plotly-latest.min.js"></script>'
                    if window_mode and not self.options.get("plotly_fs_includejs", False)
                    else ""
                )
                body = plotly.offline.plot(
                    figure_or_data, include_plotlyjs=window_mode and self.options.get("plotly_fs_includejs", False), output_type="div"
                )
                return {"body": body, "head": head}
            else:
                self
                return {"fig": figure_or_data}
        return {}

    def pie(self, key_word_sep=" ", title=None, **kwargs):
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
        plt.title(title or self.columns[1].name)
        plt.show()
        return pie

    def plot(self, title=None, **kwargs):
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
        plt.title(title or ylabel)
        plt.ylabel(ylabel)
        return plot

    def bar(self, key_word_sep=" ", title=None, **kwargs):
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
            message = 'csv results'
            return FileResultDescriptor(filename, message=message, format='csv', **kwargs)
        else:
            return outfile.getvalue()

    def _render_pie(self, key_word_sep=" ", title=None, **kwargs):
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
        plt.title(title or self.columns[1].name)
        plt.show()
        return pie

    def _render_barh(self, key_word_sep=" ", title=None, **kwargs):
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

        ax = plt.subplot(111)
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
        plt.title(title or ylabel)

        plt.show()
        return barchart

    def _render_bar(self, key_word_sep=" ", title=None, **kwargs):
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

        # print("xlabel: {}".format(xlabel))
        # print("ylabel: {}".format(ylabel))

        dim = len(quantity_columns)
        w = 0.8
        dimw = w / dim

        ax = plt.subplot(111)
        x = plt.arange(len(self.columns[0]))
        xpos = -dimw * (len(quantity_columns) / 2)
        for y in quantity_columns:
            columnchart = plt.bar(x + xpos, y, width=dimw, align="center", **kwargs)
            xpos += dimw
        plt.xticks(range(len(self.columns[0])), self.columns[0], rotation=45)
        plt.xlabel(xlabel)
        plt.ylabel(ylabel)
        plt.title(title or ylabel)

        plt.show()
        return columnchart

    def _render_linechart(self, key_word_sep=" ", title=None, **kwargs):
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
        plt.title(title or ylabel)
        plt.xlabel(xlabel)
        plt.ylabel(ylabel)
        plt.show()
        return plot

    def _render_areachart_plotly(self, key_word_sep=" ", title=None, **kwargs):
        """Generates a pylab plot from the result set.

        ``matplotlib`` must be installed, and in an
        IPython Notebook, inlining must be on::

            %%matplotlib inline

        First column is x-axis, and should be a numeric column. Other numeric columns are y-axes.
        kind = default, unstacked, stacked, stacked100 

        Parameters
        ----------
        title: Plot title, defaults to names of Y value columns

        Any additional keyword arguments will be passsed
        through to ``matplotlib.pylab.plot``.
        """

        self._build_chart_sub_tables(x_type='first')
        if len(self.chart_sub_tables) < 1:
            return None
        ylabel_names = []
        for tab in self.chart_sub_tables:
            if tab.col_y.name not in ylabel_names:
                ylabel_names.append(tab.col_y.name)
        ylabel = ", ".join(ylabel_names)
        xlabel = self.chart_sub_tables[0].col_x.name
        n_colors = len(self.chart_sub_tables)

        data = [
            go.Scatter(
                x=list(tab.keys()),
                y=list(tab.values()),
                name=tab.name,
                mode="lines",
                line=dict(width=0.5, color=self.get_color_from_palette(idx, n_colors=n_colors)),
                fill="tozeroy",
            )
            for idx, tab in enumerate(self.chart_sub_tables)
        ]
        layout = go.Layout(
            title=title or "areachart",
            showlegend=True,
            xaxis=dict(title=xlabel, type="category"),
            yaxis=dict(
                title=ylabel,
                type="linear",
                # range=[0, 3],
                # dtick=20,
                ticksuffix="",
            ),
        )
        fig = go.FigureWidget(data=data, layout=layout)
        return fig

    def _render_stackedareachart_plotly(self, key_word_sep=" ", title=None, **kwargs):
        """Generates a pylab plot from the result set.

        ``matplotlib`` must be installed, and in an
        IPython Notebook, inlining must be on::

            %%matplotlib inline

        Stacked area graph. First column is x-axis, and should be a datetime column. Other numeric columns are y-axes.

        Parameters
        ----------
        title: Plot title, defaults to names of Y value columns

        Any additional keyword arguments will be passsed
        through to ``matplotlib.pylab.plot``.
        """


        self._build_chart_sub_tables(x_type='first')
        if len(self.chart_sub_tables) < 1:
            return None
        ylabel_names = []
        for tab in self.chart_sub_tables:
            if tab.col_y.name not in ylabel_names:
                ylabel_names.append(tab.col_y.name)
        ylabel = ", ".join(ylabel_names)
        xlabel = self.chart_sub_tables[0].col_x.name
        n_colors = len(self.chart_sub_tables)

        ys_stcks = []
        y_stck = [0 for x in range(len(self.chart_sub_tables[0]))]
        for tab in self.chart_sub_tables:
            y_stck = [(r or 0) + y_stck[idx]  for (idx, r) in enumerate(tab.values())]
            ys_stcks.append(y_stck)

        data = [
            go.Scatter(
                x=list(tab.keys()),
                y=ys_stcks[idx],
                name=tab.name,
                mode="lines",
                line=dict(width=0.5, color=self.get_color_from_palette(idx, n_colors=n_colors)),
                fill="tonexty",
            )
            for idx, tab in enumerate(self.chart_sub_tables)
        ]
        layout = go.Layout(
            title=title or "stackedareachart",
            showlegend=True,
            xaxis=dict(title=xlabel, type="date"),
            yaxis=dict(
                title=ylabel,
                type="linear",
                # range=[0, 3],
                # dtick=20,
                ticksuffix="",
            ),
        )
        fig = go.FigureWidget(data=data, layout=layout)
        return fig

    def _render_timechart_plotly(self, key_word_sep=" ", title=None, **kwargs):
        """Generates a pylab plot from the result set.

        ``matplotlib`` must be installed, and in an
        IPython Notebook, inlining must be on::

            %%matplotlib inline

        Line graph. First column is x-axis, and should be datetime. Other columns are y-axes.

        Parameters
        ----------
        title: Plot title, defaults to names of Y value columns

        Any additional keyword arguments will be passsed
        through to ``matplotlib.pylab.plot``.
        """

        self._build_chart_sub_tables(x_type='datetime')
        if len(self.chart_sub_tables) < 1:
            return None
        ylabel_names = []
        for tab in self.chart_sub_tables:
            if tab.col_y.name not in ylabel_names:
                ylabel_names.append(tab.col_y.name)
        ylabel = ", ".join(ylabel_names)
        xlabel = self.chart_sub_tables[0].col_x.name
        n_colors = len(self.chart_sub_tables)

        data = [
            go.Scatter(
                x=list(tab.keys()), 
                y=list(tab.values()), 
                name=tab.name, 
                line=dict(width=1, color=self.get_color_from_palette(idx, n_colors=n_colors)), 
                opacity=0.8
            )
            for idx, tab in enumerate(self.chart_sub_tables)
        ]

        layout = go.Layout(
            title=title or "timechart",
            showlegend=True,
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
                title=xlabel,
                type="date",
            ),
            yaxis=dict(
                title=ylabel,
                type="linear",
                # range=[0, 3],
                # dtick=20,
                ticksuffix="",
            ),
        )
        fig = go.FigureWidget(data=data, layout=layout)
        return fig

    def _render_piechart_plotly(self, key_word_sep=" ", title=None, **kwargs):

        self._build_chart_sub_tables(x_type='first')
        if len(self.chart_sub_tables) < 1:
            return None
        ylabel_names = []
        for tab in self.chart_sub_tables:
            if tab.col_y.name not in ylabel_names:
                ylabel_names.append(tab.col_y.name)
        ylabel = ", ".join(ylabel_names)
        xlabel = self.chart_sub_tables[0].col_x.name
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
        data = [
            go.Pie(
                labels=list(tab.keys()), 
                values=list(tab.values()), 
                domain=domains[idx], 
                marker=dict(colors=palette), 
                name=tab.name, 
                textinfo="label+percent")
            for idx, tab in enumerate(self.chart_sub_tables)
        ]
        layout = go.Layout(
            title=title or "piechart",
            showlegend=True,
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
        fig = go.FigureWidget(data=data, layout=layout)
        return fig

    def _render_barchart_plotly(self, key_word_sep=" ", title=None, **kwargs):

        self._build_chart_sub_tables(x_type='first')
        if len(self.chart_sub_tables) < 1:
            return None
        ylabel_names = []
        for tab in self.chart_sub_tables:
            if tab.col_y.name not in ylabel_names:
                ylabel_names.append(tab.col_y.name)
        ylabel = ", ".join(ylabel_names)
        xlabel = self.chart_sub_tables[0].col_x.name
        n_colors = len(self.chart_sub_tables)

        data = [
            go.Bar(x=list(tab.values()), 
                   y=list(tab.keys()), 
                   marker=dict(color=self.get_color_from_palette(idx, n_colors=n_colors)), 
                   name=tab.name, 
                   orientation="h",)
            for idx, tab in enumerate(self.chart_sub_tables)
        ]
        layout = go.Layout(
            title=title or "barchart",
            showlegend=True,
            xaxis=dict(
                title=ylabel,
                type="linear",
                # range=[0, 3],
                # dtick=20,
                ticksuffix="",
            ),
            yaxis=dict(type="category", title=xlabel),
        )
        fig = go.FigureWidget(data=data, layout=layout)
        return fig

    def _render_columnchart_plotly(self, key_word_sep=" ", title=None, **kwargs):

        self._build_chart_sub_tables(x_type='first')
        if len(self.chart_sub_tables) < 1:
            return None
        ylabel_names = []
        for tab in self.chart_sub_tables:
            if tab.col_y.name not in ylabel_names:
                ylabel_names.append(tab.col_y.name)
        ylabel = ", ".join(ylabel_names)
        xlabel = self.chart_sub_tables[0].col_x.name
        n_colors = len(self.chart_sub_tables)

        data = [
            go.Bar(
                x=list(tab.keys()), 
                y=list(tab.values()), 
                marker=dict(color=self.get_color_from_palette(idx, n_colors=n_colors)), 
                name=tab.name,)
            for idx, tab in enumerate(self.chart_sub_tables)
        ]
        layout = go.Layout(
            title=title or "columnchart",
            showlegend=True,
            xaxis=dict(title=xlabel, type="category"),
            yaxis=dict(
                title=ylabel,
                type="linear",
                # range=[0, 3],
                # dtick=20,
                ticksuffix="",
            ),
        )
        fig = go.FigureWidget(data=data, layout=layout)
        return fig

    def _render_linechart_plotly(self, key_word_sep=" ", title=None, **kwargs):
        """Generates a pylab plot from the result set.

        ``matplotlib`` must be installed, and in an
        IPython Notebook, inlining must be on::

            %%matplotlib inline

        Line graph. First column is x-axis, and should be numeric. Other columns are y-axes.

        Parameters
        ----------
        title: Plot title, defaults to names of Y value columns

        Any additional keyword arguments will be passsed
        through to ``matplotlib.pylab.plot``.
        """

        self._build_chart_sub_tables(x_type='quantity')
        if len(self.chart_sub_tables) < 1:
            return None
        ylabel_names = []
        for tab in self.chart_sub_tables:
            if tab.col_y.name not in ylabel_names:
                ylabel_names.append(tab.col_y.name)
        ylabel = ", ".join(ylabel_names)
        xlabel = self.chart_sub_tables[0].col_x.name
        n_colors = len(self.chart_sub_tables)

        data = [
            go.Scatter(
                x=list(tab.keys()), 
                y=list(tab.values()), 
                name=tab.name, 
                line=dict(width=1, color=self.get_color_from_palette(idx, n_colors=n_colors)), 
                opacity=0.8
            )
            for idx, tab in enumerate(self.chart_sub_tables)
        ]
        layout = go.Layout(
            title=title or "linechart",
            showlegend=True,
            xaxis=dict(
                title=xlabel,
                # type='linear',
            ),
            yaxis=dict(
                title=ylabel,
                type="linear",
                # range=[0, 3],
                # dtick=20,
                # ticksuffix=''
            ),
        )
        fig = go.FigureWidget(data=data, layout=layout)
        return fig

    def _render_scatterchart_plotly(self, key_word_sep=" ", title=None, **kwargs):
        """Generates a pylab plot from the result set.

        ``matplotlib`` must be installed, and in an
        IPython Notebook, inlining must be on::

            %%matplotlib inline

        First column is x-axis, and should be a numeric column. Other numeric columns are y-axes.
        kind = default, unstacked, stacked, stacked100 

        Parameters
        ----------
        title: Plot title, defaults to names of Y value columns

        Any additional keyword arguments will be passsed
        through to ``matplotlib.pylab.plot``.
        """

        self.build_columns()
        quantity_columns = [c for c in self.columns[1:] if c.is_quantity]
        if len(quantity_columns) < 1:
            return None

        xticks = self.columns[0]
        ys = quantity_columns

        ylabel = ", ".join([c.name for c in ys])
        xlabel = xticks.name
        data = [
            go.Scatter(
                x=xticks,
                y=yticks,
                name=yticks.name,
                mode="markers",
                marker=dict(line=dict(width=1), color=self.get_color_from_palette(idx, n_colors=len(ys))),
            )
            for idx, yticks in enumerate(ys)
        ]
        layout = go.Layout(
            title=title or "scatterchart",
            showlegend=True,
            xaxis=dict(title=xlabel, type="category"),
            yaxis=dict(
                title=ylabel,
                type="linear",
                # range=[0, 3],
                # dtick=20,
                ticksuffix="",
            ),
        )
        fig = go.FigureWidget(data=data, layout=layout)
        return fig

    def _render_anomalychart_plotly(self, key_word_sep=" ", title=None, **kwargs):
        return self._render_timechart_plotly(key_word_sep, title or "anomalychart", fieldnames="kql-anomalychart-plot", **kwargs)


class PrettyTable(prettytable.PrettyTable):

    # Object constructor
    def __init__(self, *args, **kwargs):
        self.row_count = 0
        self.display_limit = None
        return super(PrettyTable, self).__init__(*args, **kwargs)

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
            self.add_row(row)

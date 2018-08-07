import functools
import operator
import csv
import six
import codecs
import os.path
import re
import prettytable
from kql.column_guesser import ColumnGuesserMixin

from kql.display import Display

import plotly
plotly.offline.init_notebook_mode(connected=True)

import plotly.plotly as py
import plotly.graph_objs as go



def _unduplicate_field_names(field_names):
    """Append a number to duplicate field names to make them unique. """
    res = []
    for k in field_names:
        if k in res:
            i = 1
            while k + '_' + str(i) in res:
                i += 1
            k += '_' + str(i)
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
            _row = [s.encode("utf-8")
                    if hasattr(s, "encode")
                    else s
                    for s in row]
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

class CsvResultDescriptor(object):
    """Provides IPython Notebook-friendly output for the feedback after a ``.csv`` called."""

    # Object constructor
    def __init__(self, file_path):
        self.file_path = file_path

    # Printable unambiguous presentation of the object
    def __repr__(self):
        return 'CSV results at %s' % os.path.join(os.path.abspath('.'), self.file_path)

    # IPython html presentation of the object
    def _repr_html_(self):
        return '<a href="%s">CSV results</a>' % os.path.join('.', 'files', self.file_path)




def _nonbreaking_spaces(match_obj):
    """
    Make spaces visible in HTML by replacing all `` `` with ``&nbsp;``

    Call with a ``re`` match object.  Retain group 1, replace group 2
    with nonbreaking speaces.
    """
    spaces = '&nbsp;' * len(match_obj.group(2))
    return '%s%s' % (match_obj.group(1), spaces)

_cell_with_spaces_pattern = re.compile(r'(<td>)( {2,})')



class ResultSet(list, ColumnGuesserMixin):
    """
    Results of a query.

    Can access rows listwise, or by string value of leftmost column.
    """

    # Object constructor
    def __init__(self, queryResult, query, options):
        self.info = []
        self.conn_info = []
        # list of columns_name
        self.columns_name = queryResult.keys()

        # query
        self.query = query
        self.options = options

        # metadata
        self.start_time = None
        self.end_time = None
        self.elapsed_timespan = None
        self.connection = None

        self._dataframe = None

        # table printing style to any of prettytable's defined styles (currently DEFAULT, MSWORD_FRIENDLY, PLAIN_COLUMNS, RANDOM)
        self.prettytable_style = prettytable.__dict__[self.options.get("prettytable_style", "DEFAULT").upper()]

        self.display_info = False
        self.suppress_result = False
        self._update(queryResult)

    def _update(self, queryResult):
        if queryResult.returns_rows:
            auto_limit = 0 if not self.options.get("auto_limit") else self.options.get("auto_limit")
            if auto_limit > 0:
                list.__init__(self, queryResult.fetchmany(size = auto_limit))
            else:
                list.__init__(self, queryResult.fetchall())

            self.field_names = _unduplicate_field_names(self.columns_name)
            self.pretty = PrettyTable(self.field_names, style=self.prettytable_style)
            self.records_count = queryResult.recordscount()
            self.visualization = queryResult.visualization_property("Visualization")
            self.title = queryResult.visualization_property("Title")
        else:
            list.__init__(self, [])
            self.pretty = None


    # IPython html presentation of the object
    def _repr_html_(self):
        self.html_body = []
        self.html_head = []
        if self.display_info:
            Display.showInfoMessage(self.conn_info)
            # msg_html = Display.getInfoMessageHtml(self.conn_info)
            # self.html_body.append(msg_html.get("body", ""))
            # self.html_head.append(msg_html.get("head", ""))
        if not self.suppress_result:
            if self.is_chart():
                self.show_chart(**self.options)
                # char_html = self._getChartHtml()
                # self.html_body.append(char_html.get("body", ""))
                # self.html_head.append(char_html.get("head", ""))
            else:
                self.show_table(**self.options)
                # table_html = self._getTableHtml()
                # self.html_body.append(table_html.get("body", ""))
                # self.html_head.append(table_html.get("head", ""))

        if self.display_info:
            Display.showInfoMessage(self.info)
            # msg_html = Display.getInfoMessageHtml(self.info)
            # b = msg_html.get("body", "")
            # if not self.suppress_result and len(b) > 0 and not self.is_chart():
                #    b = "<br>" + b
            # self.html_body.append(b)
            # self.html_head.append(msg_html.get("head", ""))
        self.display_info = False
        self.suppress_result = False
        # if len(self.html_body) > 0:
        #     html_body_str = ''.join(self.html_body)
        #     html_head_str = ''.join(self.html_head) if len(self.html_head) > 0 else ''
        #     Display.show(Display.toHtml(body = html_body_str, head = html_head_str))
        return ''


    def _getTableHtml(self):
        "get query result in a table format as an HTML string"
        _cell_with_spaces_pattern = re.compile(r'(<td>)( {2,})')
        if self.pretty:
            self.pretty.add_rows(self)
            result = self.pretty.get_html_string()
            result = _cell_with_spaces_pattern.sub(_nonbreaking_spaces, result)
            display_limit = 0 if not self.options.get("display_limit") else self.options.get("display_limit")
            if display_limit > 0 and len(self) > display_limit:
                result = '%s\n<span style="font-style:italic;text-align:center;">%d rows, truncated to display_limit of %d</span>' % (
                    result, len(self), display_limit)
            return {"body" : result}
        else:
            return {}

    def show_table(self, **kwargs):
        "display the table"
        options = {**self.options, **kwargs}
        if options.get("table_package","").upper() == "PANDAS":
            t = self.to_dataframe()._repr_html_()
            html = Display.toHtml(body = t)
        else:
            t = self._getTableHtml()
            html = Display.toHtml(**t)
        if options.get("popup_window") and not options.get("botton_text"):
            options["botton_text"] = 'popup ' + 'table'            + ((' - ' + self.title) if self.title else '') + ' '
        Display.show(html, **options)
        return None

    def popup_table(self, **kwargs):
        "display the table"
        return self.show_table(**{"popup_window" : True, **kwargs})

    # Printable pretty presentation of the object
    def __str__(self, *args, **kwargs):
        self.pretty.add_rows(self)
        return str(self.pretty or '')


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
        return dict(zip(self.columns_name, zip(*self)))


    def dicts_iterator(self):
        "Iterator yielding a dict for each row"
        for row in self:
            yield dict(zip(self.columns_name, row))


    def to_dataframe(self):
        "Returns a Pandas DataFrame instance built from the result set."
        if self._dataframe is None:
            import pandas as pd
            frame = pd.DataFrame(self, columns=(self and self.columns_name) or [])
            self._dataframe = frame
        return self._dataframe

    def submit(self):
        "display the chart that was specified in the query"
        magic = self.magic
        user_ns = magic.shell.user_ns.copy()
        result = magic.execute_query(self.parsed, user_ns)
        return result

    def refresh(self):
        "refresh the results of the query"
        magic = self.magic
        user_ns = magic.shell.user_ns.copy()
        self.conn_info = []
        self.info = []
        result = magic.execute_query(self.parsed, user_ns, self)
        return result

    def show_chart(self, **kwargs):
        "display the chart that was specified in the query"
        options = {**self.options, **kwargs}
        window_mode = options is not None and options.get("popup_window")
        if window_mode and not options.get("botton_text"):
            options["botton_text"] = 'popup ' + self.visualization + ((' - ' + self.title) if self.title else '') + ' '
        c = self._getChartHtml(window_mode)
        if c is not None:
            html = Display.toHtml(**c)
            Display.show(html, **options)
            return None
        else:
            return self.show_table(**kwargs)

    def popup_Chart(self, **kwargs):
        "display the chart that was specified in the query"
        return self.show_chart(**{"popup_window" : True, **kwargs})

    def popup(self, **kwargs):
        "display the chart that was specified in the query"
        return self.show_chart(**{"popup_window" : True, **kwargs})


    def is_chart(self):
        return self.visualization and not self.visualization == 'table'

    def _getChartHtml(self, window_mode = False):
        "get query result in a char format as an HTML string"
        # https://kusto.azurewebsites.net/docs/queryLanguage/query_language_renderoperator.html

        if not self.is_chart():
            return None

        figure_or_data = None
        # First column is color-axis, second column is numeric
        if self.visualization == 'piechart':
            figure_or_data = self._render_piechart_plotly(" ", self.title) 
            #chart = self._render_pie(" ", self.title)
        # First column is x-axis, and can be text, datetime or numeric. Other columns are numeric, displayed as horizontal strips.
        # kind = default, unstacked, stacked, stacked100 (Default, same as unstacked; unstacked - Each "area" to its own; stacked - "Areas" are stacked to the right; stacked100 - "Areas" are stacked to the right, and stretched to the same width)
        elif self.visualization == 'barchart':
            figure_or_data = self._render_barchart_plotly(" ", self.title)
            # chart = self._render_barh(" ", self.title)
        # Like barchart, with vertical strips instead of horizontal strips.
        # kind = default, unstacked, stacked, stacked100 
        elif self.visualization == 'columnchart':
            figure_or_data = self._render_columnchart_plotly(" ", self.title)
            # chart = self._render_bar(" ", self.title)
         # Area graph. First column is x-axis, and should be a numeric column. Other numeric columns are y-axes.
         # kind = default, unstacked, stacked, stacked100 
        elif self.visualization == 'areachart':
            figure_or_data = self._render_areachart_plotly(" ", self.title) 
            # chart = self._render_areachart(" ", self.title)
        # Line graph. First column is x-axis, and should be a numeric column. Other numeric columns are y-axes.
        elif self.visualization == 'linechart':
            figure_or_data = self._render_linechart_plotly(" ", self.title)
        # Line graph. First column is x-axis, and should be datetime. Other columns are y-axes.
        elif self.visualization == 'timechart':
            figure_or_data = self._render_timechart_plotly(" ", self.title)
        # Similar to timechart, but highlights anomalies using an external machine-learning service.
        elif self.visualization == 'anomalychart':
            figure_or_data = self._render_anomalychart_plotly(" ", self.title)
        # Stacked area graph. First column is x-axis, and should be a numeric column. Other numeric columns are y-axes.
        elif self.visualization == 'stackedareachart':
            figure_or_data = self._render_stackedareachart_plotly(" ", self.title)
        # Last two columns are the x-axis, other columns are y-axis.
        elif self.visualization == 'ladderchart':
            figure_or_data = self.pie(" ", self.title)
        # Interactive navigation over the events time-line (pivoting on time axis)
        elif self.visualization == 'timepivot':
            figure_or_data = self.pie(" ", self.title)
        # Displays a pivot table and chart. User can interactively select data, columns, rows and various chart types.
        elif self.visualization == 'pivotchart':
            figure_or_data = self.pie(" ", self.title)
        # Points graph. First column is x-axis, and should be a numeric column. Other numeric columns are y-axes
        elif self.visualization == 'scatterchart':
            figure_or_data = self._render_scatterchart_plotly(" ", self.title)

        if figure_or_data is not None:
            head = '<script src="https://cdn.plot.ly/plotly-latest.min.js"></script>' if window_mode and not self.options.get("plotly_fs_includejs", False) else ""
            body = plotly.offline.plot(figure_or_data, include_plotlyjs= window_mode and self.options.get("plotly_fs_includejs", False), output_type='div')
            return {"body" : body, "head" : head}
        return None



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
        if hasattr(self.x, 'name'):
            plt.xlabel(self.x.name)
        ylabel = ", ".join(y.name for y in self.ys)
        plt.title(title or ylabel)
        plt.ylabel(ylabel)
        return plot

    def bar(self, key_word_sep = " ", title=None, **kwargs):
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
            plt.xticks(range(len(self.xlabels)), self.xlabels,
                       rotation=45)
        plt.xlabel(self.xlabel)
        plt.ylabel(self.ys[0].name)
        return plot

    def to_csv(self, filename=None, **format_params):
        """Generate results in comma-separated form.  Write to ``filename`` if given.
           Any other parameters will be passed on to csv.writer."""
        if not self.pretty:
            return None # no results
        self.pretty.add_rows(self)
        if filename:
            encoding = format_params.get('encoding', 'utf-8')
            if six.PY2:
                outfile = open(filename, 'wb')
            else:
                outfile = open(filename, 'w', newline='', encoding=encoding)
        else:
            outfile = six.StringIO()
        writer = UnicodeWriter(outfile, **format_params)
        writer.writerow(self.field_names)
        for row in self:
            writer.writerow(row)
        if filename:
            outfile.close()
            return CsvResultDescriptor(filename)
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
            barchart = plt.barh(x + xpos, y, align='center', **kwargs)
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
            columnchart = plt.bar(x + xpos, y, width = dimw, align='center', **kwargs)
            xpos += dimw
        plt.xticks(range(len(self.columns[0])), self.columns[0], rotation = 45)
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

        colors_pallete = ['rgb(184, 247, 212)', 'rgb(111, 231, 219)', 'rgb(127, 166, 238)', 'rgb(131, 90, 241)' ]

        self.build_columns()
        quantity_columns = [c for c in self.columns[1:] if c.is_quantity]
        if len(quantity_columns) < 1:
            return None

        xticks = self.columns[0]
        ys = quantity_columns
        ylabel = ", ".join([c.name for c in ys])
        xlabel = xticks.name
        data = [go.Scatter(x=xticks,y=yticks,name=yticks.name,mode='lines',line=dict(width=0.5,color=colors_pallete[idx % len(colors_pallete)]),fill='tozerox') for idx, yticks in enumerate(ys)]
        layout = go.Layout(
            title = title or "areachart",
            showlegend=True,
            xaxis=dict(
                title=xlabel,
                type='category',
            ),
            yaxis=dict(
                title=ylabel,
                type='linear',
                # range=[0, 3],
                # dtick=20,
                ticksuffix=''
            )
        )
        fig = go.Figure(data=data, layout=layout)
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

        colors_pallete = ['rgb(184, 247, 212)', 'rgb(111, 231, 219)', 'rgb(127, 166, 238)', 'rgb(131, 90, 241)' ]

        self.build_columns()
        quantity_columns = [c for c in self.columns if c.is_quantity]
        if len(quantity_columns) < 2:
            return None
        if not quantity_columns[0].is_datetime:
            return None

        xticks = quantity_columns[0]
        ys = quantity_columns[1:]
        ys_stcks = []
        y_stck = [0 for x in range(len(ys[0]))]
        for y in ys:
            y_stck = [r + y_stck[idx] for (idx, r) in enumerate(y)]
            ys_stcks.append(y_stck)
        ylabel = ", ".join([c.name for c in ys])
        xlabel = xticks.name
        data = [go.Scatter(x=xticks,y=yticks,name=ys[idx].name,mode='lines',line=dict(width=0.5,color=colors_pallete[idx % len(colors_pallete)]),fill='tonexty') for idx, yticks in enumerate(ys_stcks)]
        layout = go.Layout(
            title = title or "stackedareachart",
            showlegend=True,
            xaxis=dict(
                title=xlabel,
                type='date',
            ),
            yaxis=dict(
                title=ylabel,
                type='linear',
                # range=[0, 3],
                # dtick=20,
                ticksuffix=''
            )
        )
        fig = go.Figure(data=data, layout=layout)
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

        colors_pallete = ['rgb(184, 247, 212)', 'rgb(111, 231, 219)', 'rgb(127, 166, 238)', 'rgb(131, 90, 241)' ]

        self.build_columns()
        quantity_columns = [c for c in self.columns if c.is_quantity]
        if len(quantity_columns) < 2:
            return None
        if not quantity_columns[0].is_datetime:
            return None

        xticks = quantity_columns[0]
        ys = quantity_columns[1:]
        ylabel = ", ".join([c.name for c in ys])
        xlabel = xticks.name
        data = [go.Scatter(
                                x=xticks,
                                y=yticks,
                                name = yticks.name,
                                line=dict(
                                    width=0.5,
                                    color=colors_pallete[idx % len(colors_pallete)]
                                ),
                                opacity = 0.8
                          ) 
                for idx, yticks in enumerate(ys)]
        layout = go.Layout(
            title = title or "timechart",
            showlegend=True,
            xaxis=dict(
                rangeselector=dict(
                    buttons=list([
                        dict(count=1,
                             label='1m',
                             step='month',
                             stepmode='backward'),
                        dict(count=6,
                             label='6m',
                             step='month',
                             stepmode='backward'),
                        dict(step='all')
                    ])
                ),
                rangeslider=dict(),
                title=xlabel,
                type='date'
            ),
           yaxis=dict(
                title=ylabel,
                type='linear',
                # range=[0, 3],
                # dtick=20,
                ticksuffix=''
            )
        )
        fig = go.Figure(data=data, layout=layout)
        return fig


    def _render_piechart_plotly(self, key_word_sep=" ", title=None, **kwargs):

        colors_pallete = ['rgb(184, 247, 212)', 'rgb(111, 231, 219)', 'rgb(127, 166, 238)', 'rgb(131, 90, 241)' ]

        self.build_columns()
        quantity_columns = [c for c in self.columns[1:] if c.is_quantity]
        if len(quantity_columns) < 1:
            return None

        pies = len(quantity_columns)
        ydelta = 1.0 / ((pies + 1) // 2)

        xticks = self.columns[0]
        ys = quantity_columns
        ylabel = ", ".join([c.name for c in ys])
        xlabel = xticks.name
        domains = [dict(x = [0, 1], y = [0, 1])] if pies == 1 else [dict(x = [0 + .52 *(i % 2), 1 - .52*(1 - i % 2)], y = [(i // 2) * ydelta, ((i // 2 + 1) - 0.05) * ydelta ]) for i in range(0, pies)]
        data = [go.Pie(labels=xticks, values=yticks, domain=domains[idx], marker=dict(colors=colors_pallete), name=yticks.name, textinfo='label+percent') for idx, yticks in enumerate(ys)]
        layout = go.Layout(
            title = title or "piechart",
            showlegend=True
        )
        layout1 = go.Layout(
            title = title or "piechart",
            showlegend=True,
            xaxis=dict(
                title=xlabel,
                type='category',
            ),
            yaxis=dict(
                title=ylabel,
                type='linear',
                # range=[0, 3],
                # dtick=20,
                ticksuffix=''
            )
        )
        fig = go.Figure(data=data, layout=layout)
        return fig

    def _render_barchart_plotly(self, key_word_sep=" ", title=None, **kwargs):

        colors_pallete = ['rgb(184, 247, 212)', 'rgb(111, 231, 219)', 'rgb(127, 166, 238)', 'rgb(131, 90, 241)' ]

        self.build_columns()
        quantity_columns = [c for c in self.columns[1:] if c.is_quantity]
        if len(quantity_columns) < 1:
            return None

        xticks = self.columns[0]
        ys = quantity_columns
        ylabel = ", ".join([c.name for c in ys])
        xlabel = xticks.name
        data = [go.Bar(x=yticks, y=xticks, marker=dict(color=colors_pallete[idx % len(colors_pallete)]), name=yticks.name, orientation = 'h') for idx, yticks in enumerate(ys)]
        layout = go.Layout(
            title = title or "barchart",
            showlegend=True,
            xaxis=dict(
                title=ylabel,
                type='linear',
                # range=[0, 3],
                # dtick=20,
                ticksuffix=''
            ),
            yaxis=dict(
                type='category',
                title=xlabel,
            )
        )
        fig = go.Figure(data=data, layout=layout)
        return fig

    def _render_columnchart_plotly(self, key_word_sep=" ", title=None, **kwargs):

        colors_pallete = ['rgb(184, 247, 212)', 'rgb(111, 231, 219)', 'rgb(127, 166, 238)', 'rgb(131, 90, 241)' ]

        self.build_columns()
        quantity_columns = [c for c in self.columns[1:] if c.is_quantity]
        if len(quantity_columns) < 1:
            return None

        xticks = self.columns[0]
        ys = quantity_columns
        ylabel = ", ".join([c.name for c in ys])
        xlabel = xticks.name
        data = [go.Bar(x=xticks, y=yticks, marker=dict(color=colors_pallete[idx % len(colors_pallete)]), name=yticks.name) for idx, yticks in enumerate(ys)]
        layout = go.Layout(
            title = title or "columnchart",
            showlegend=True,
            xaxis=dict(
                title=xlabel,
                type='category',
            ),
            yaxis=dict(
                title=ylabel,
                type='linear',
                # range=[0, 3],
                # dtick=20,
                ticksuffix=''
            )
        )
        fig = go.Figure(data=data, layout=layout)
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

        colors_pallete = ['rgb(184, 247, 212)', 'rgb(111, 231, 219)', 'rgb(127, 166, 238)', 'rgb(131, 90, 241)' ]

        self.build_columns()
        quantity_columns = [c for c in self.columns if c.is_quantity]
        if len(quantity_columns) < 2:
            return None

        self.build_columns(quantity_columns[0].name)
        quantity_columns = [c for c in self.columns if c.is_quantity]

        xticks = quantity_columns[0]
        ys = quantity_columns[1:]
        ylabel = ", ".join([c.name for c in ys])
        xlabel = xticks.name
        data = [go.Scatter(
                                x=xticks,
                                y=yticks,
                                name = yticks.name,
                                line=dict(
                                    width=1,
                                    color=colors_pallete[idx % len(colors_pallete)]
                                ),
                                opacity = 0.8
                          ) 
                for idx, yticks in enumerate(ys)]
        layout = go.Layout(
            title = title or "linechart",
            showlegend=True,
            xaxis=dict(
                title=xlabel,
                # type='linear',
            ),
           yaxis=dict(
                title=ylabel,
                type='linear',
                # range=[0, 3],
                # dtick=20,
                # ticksuffix=''
            )
        )
        fig = go.Figure(data=data, layout=layout)
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


        colors_pallete = ['rgb(184, 247, 212)', 'rgb(111, 231, 219)', 'rgb(127, 166, 238)', 'rgb(131, 90, 241)' ]

        self.build_columns()
        quantity_columns = [c for c in self.columns[1:] if c.is_quantity]
        if len(quantity_columns) < 1:
            return None

        xticks = self.columns[0]
        ys = quantity_columns

        ylabel = ", ".join([c.name for c in ys])
        xlabel = xticks.name
        data = [go.Scatter(x=xticks,y=yticks,name=yticks.name,mode='markers',marker=dict(line=dict(width=1),color=colors_pallete[idx % len(colors_pallete)])) for idx, yticks in enumerate(ys)]
        layout = go.Layout(
            title = title or "scatterchart",
            showlegend=True,
            xaxis=dict(
                title=xlabel,
                type='category',
            ),
            yaxis=dict(
                title=ylabel,
                type='linear',
                # range=[0, 3],
                # dtick=20,
                ticksuffix=''
            )
        )
        fig = go.Figure(data=data, layout=layout)
        return fig



    def _render_anomalychart_plotly(self, key_word_sep=" ", title=None, **kwargs):
        return self._render_timechart_plotly(key_word_sep, title or "anomalychart", fieldnames='kql-anomalychart-plot', **kwargs)


class PrettyTable(prettytable.PrettyTable):

    # Object constructor
    def __init__(self, *args, **kwargs):
        self.row_count = 0
        self.display_limit = None
        return super(PrettyTable, self).__init__(*args,  **kwargs)

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
        for row in data[:self.display_limit]:
            self.add_row(row)



#-------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
#--------------------------------------------------------------------------

from datetime import timedelta, datetime


"""
Splits tabular data in the form of a list of rows into columns;
makes guesses about the role of each column for plotting purposes
(X values, Y values, and text labels).
"""


class Column(list):
    "Store a column of tabular data; record its name and whether it is numeric"
    # Object constructor
    def __init__(self, idx=None,name='', col=None, is_quantity=True, is_datetime=True, **kwargs):
        if col is not None:
            self.is_quantity = col.is_quantity
            self.is_datetime = col.is_datetime
            self.idx = col.idx
            self.name = col.name
        else:
            self.is_quantity = is_quantity
            self.is_datetime = is_datetime
            self.idx = idx
            self.name = name
        super(Column, self).__init__(**kwargs)

class ChartSubTable(dict):
    def __init__(self, col_x=None, col_y=None, name=None, mapping=None, **kwargs):
        self.col_x = col_x
        self.col_y = col_y
        self.name = name
        super(ChartSubTable, self).__init__(**kwargs)
        if mapping:
            self.update(mapping)


def is_quantity(val):
    """Is ``val`` a quantity (int, float, datetime, etc) (not str, bool)?
    
    Relies on presence of __sub__.
    """
    return hasattr(val, "__sub__")


class ColumnGuesserMixin(object):
    """
    plot: [x, y, y...], y
    pie: ... y
    """

    DATAFRAME_QUNATITY_TYPES = ["int64", "float64", "datetime64[ns]", "timedelta64[ns]", "int32",]
    DATAFRAME_TIME_TYPES = ["datetime64[ns]", "timedelta64[ns]",]

    def _build_chart_sub_tables(self, name=None):
        self._build_columns(name, without_data=True)
        quantity_columns = [c for c in self.columns[1:] if c.is_quantity]
        non_quantity_columns = [c for c in self.columns[1:] if not c.is_quantity]

        rows = self

        if self.columns[0].is_quantity:
            rows = sorted(self, key=lambda row: row[0])

        col_x = Column(col=self.columns[0])
        for row in rows:
            if row[0] not in col_x:
                col_x.append(row[0])

        chart_sub_tables_dict = {}
        for row in rows:
            for qcol in quantity_columns:
                if len(non_quantity_columns) > 0:
                    sub_table_name = ':'.join([row[col.idx] for col in non_quantity_columns]) + ':' + qcol.name
                else:
                    sub_table_name =  qcol.name
                chart_sub_table = chart_sub_tables_dict.get(sub_table_name)
                if chart_sub_table is None:
                    chart_sub_table = chart_sub_tables_dict[sub_table_name] = ChartSubTable(
                        name=sub_table_name, 
                        col_x=Column(col=self.columns[0]), 
                        col_y=Column(col=qcol),
                        mapping=dict(zip(col_x, [None for i in range(len(col_x))])))
                chart_sub_table[row[0]] = row[qcol.idx]
        self.chart_sub_tables = list(chart_sub_tables_dict.values())

    def _build_columns(self, name=None, without_data=False):
        self.x = Column()
        self.ys = []

        rows = self
        if name:
            idx = self.columns_name.index(name)
            rows = sorted(self, key=lambda row: row[idx])  # sort by index

        self.columns = [Column(idx, name) for (idx, name) in enumerate(self.columns_name)]
        if len(self.columns_datafarme_type) > 0:
            for col in self.columns:
                datafarme_type = self.columns_datafarme_type[col.idx]
                col.is_quantity = datafarme_type in self.DATAFRAME_QUNATITY_TYPES
                col.is_datetime = datafarme_type in self.DATAFRAME_TIME_TYPES
            if without_data:
                return

        for row in rows:
            for (col_idx, col_val) in enumerate(row):
                col = self.columns[col_idx]
                if not without_data:
                    col.append(col_val)
                if len(self.columns_datafarme_type) == 0:
                    if (col_val is not None) and (not is_quantity(col_val)):
                        col.is_quantity = False
                        col.is_datetime = False
                    elif not isinstance(col_val, datetime):
                        col.is_datetime = False


    def _get_y(self):
        for idx in range(len(self.columns) - 1, -1, -1):
            if self.columns[idx].is_quantity:
                self.ys.insert(0, self.columns.pop(idx))
                return True

    def _get_x(self):
        for idx in range(len(self.columns)):
            if self.columns[idx].is_quantity:
                self.x = self.columns.pop(idx)
                return True

    def _get_xlabel(self, xlabel_sep=" "):
        self.xlabels = []
        if self.columns:
            for row_idx in range(len(self.columns[0])):
                self.xlabels.append(xlabel_sep.join(str(c[row_idx]) for c in self.columns))
        self.xlabel = ", ".join(c.name for c in self.columns)

    def _get_xlabel(self, xlabel_sep=" ", index=None):
        self.xlabels = []
        if self.columns:
            for row_idx in range(len(self.columns[0])):
                self.xlabels.append(xlabel_sep.join(str(c[row_idx]) for c in self.columns))
        self.xlabel = ", ".join(c.name for c in self.columns)

    def _guess_columns(self):
        self._build_columns()
        self._get_y()
        if not self.ys:
            raise AttributeError("No quantitative columns found for chart")

    def build_columns(self, name=None):
        """
        just build the columns.
        if name specified, first sort row by name
        """
        self._build_columns(name)

    def guess_pie_columns(self, xlabel_sep=" "):
        """
        Assigns x, y, and x labels from the data set for a pie chart.
        
        Pie charts simply use the last quantity column as 
        the pie slice size, and everything else as the
        pie slice labels.
        """
        self._guess_columns()
        self._get_xlabel(xlabel_sep)

    def guess_plot_columns(self):
        """
        Assigns ``x`` and ``y`` series from the data set for a plot.
        
        Plots use:
          the rightmost quantity column as a Y series
          optionally, the leftmost quantity column as the X series
          any other quantity columns as additional Y series
        """
        self._guess_columns()
        self._get_x()
        while self._get_y():
            pass

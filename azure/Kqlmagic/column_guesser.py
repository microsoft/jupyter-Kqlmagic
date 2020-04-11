# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

from datetime import timedelta, datetime


import pytz


from .constants import Constants, VisualizationKeys


"""
Splits tabular data in the form of a list of rows into columns;
makes guesses about the role of each column for plotting purposes
(X values, Y values, and text labels).
"""


class Column(list):
    "Store a column of tabular data; record its name and whether it is numeric"

    # Object constructor
    def __init__(self, idx=None, name="", col=None, is_quantity=True, is_datetime=True, **kwargs):
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
        super(Column, self).__init__()


class ChartSubTable(dict):
    def __init__(self, col_x=None, col_y=None, name=None, mapping=None, is_descending_sorted=None, col_y_min=None, col_y_max=None,  **kwargs):
        self.is_descending_sorted = is_descending_sorted
        self.col_x = col_x
        self.col_y = col_y
        self.col_y_min = col_y_min
        self.col_y_max = col_y_max
        self.name = name
        super(ChartSubTable, self).__init__()
        if mapping:
            self.update(mapping)


def is_quantity(val) -> bool:
    """Is ``val`` a quantity (int, float, datetime, etc) (not str, bool)?
    
    Relies on presence of __sub__.
    """
    
    return hasattr(val, "__sub__")


def datetime_to_linear_ticks(t: datetime) -> int:
    return (t-datetime(1,1,1,0,0,0,0, pytz.UTC)).total_seconds() * Constants.TICK_TO_INT_FACTOR


class ColumnGuesserMixin(object):
    """
    plot: [x, y, y...], y
    pie: ... y
    """

    DATAFRAME_QUNATITY_TYPES = ["int64", "float64", "datetime64[ns]", "timedelta64[ns]", "int32"]
    DATAFRAME_TIME_TYPES = ["datetime64[ns]", "timedelta64[ns]"]

    def _build_chart_sub_tables(self, properties:dict, name=None, x_type="first") -> list:
        self.chart_sub_tables = []
        self._build_columns(name, without_data=True)

        #
        # discover column X index
        #
        x_col_idx = None
        x_col_name = properties.get(VisualizationKeys.X_COLUMN)
        if x_col_name is not None:
            for idx, c in enumerate(self.columns):
                if c.name == x_col_name:
                    x_col_idx = idx
                    break
        elif x_type == "first":
            x_col_idx = 0
        elif x_type == "quantity":
            for idx, c in enumerate(self.columns):
                if c.is_quantity:
                    if not c.is_datetime: 
                        x_col_idx = idx
                        break
                    elif x_col_idx is None:
                        x_col_idx = idx
        elif x_type == "datetime":
            # find first of type datetime
            for idx, c in enumerate(self.columns):
                if c.is_datetime:
                    x_col_idx = idx
                    break
        if x_col_idx is None:
            # this print is not for debug
            print("No valid xcolumn")
            return []

        rows = self

        #
        # discover x direction, and always sort ascending
        #
        is_descending_sorted = None
        if self.columns[x_col_idx].is_quantity and len(rows) >= 2:
            if  properties.get(VisualizationKeys.IS_QUERY_SORTED) == True:
                previous_col_value = rows[0][x_col_idx]
                is_descending_sorted = True
                for r in rows[1:]:
                    current_col_value = r[x_col_idx]
                    if previous_col_value < current_col_value:
                        is_descending_sorted = False
                        break
                    previous_col_value = current_col_value

            rows = list(reversed(list(self))) if is_descending_sorted else sorted(self, key=lambda row: row[x_col_idx])

        #
        # create a new unique list of col_x values (keep same order)
        #
        col_x = Column(col=self.columns[x_col_idx])
        for row in rows:
            if row[x_col_idx] not in col_x:
                col_x.append(row[x_col_idx])

        #
        # discover series columns (each combination of values in this columns, is a serie)
        #
        specified_series_columns = properties.get(VisualizationKeys.SERIES) or []
        series_columns = [c for idx, c in enumerate(self.columns) if c.name in specified_series_columns or (idx != x_col_idx and not c.is_quantity)]

        #
        # discover y columns
        #
        y_cols_name = properties.get(VisualizationKeys.Y_COLUMNS)
        if y_cols_name is not None:
            quantity_columns = [c for c in self.columns if c.name in y_cols_name and c.is_quantity]
        else:
            quantity_columns = [c for idx, c in enumerate(self.columns) if idx != x_col_idx and c.is_quantity and c.name not in [s.name for s in series_columns]] 
        if len(quantity_columns) < 1:
            # this print is not for debug
            print("No valid ycolumns")
            return []

        #
        # create chart sub-tables
        # a sub-table for each serie X y-col
        #
        chart_sub_tables_dict = {}
        for row in rows:
            for qcol in quantity_columns:
                if len(series_columns) > 0:
                    sub_table_name = f'{":".join([str(row[col.idx]) for col in series_columns])}:{qcol.name}'
                else:
                    sub_table_name = qcol.name
                chart_sub_table = chart_sub_tables_dict.get(sub_table_name)
                if chart_sub_table is None:
                    chart_sub_table = chart_sub_tables_dict[sub_table_name] = ChartSubTable(
                        name=sub_table_name,
                        col_x=Column(col=self.columns[x_col_idx]),
                        col_y=Column(col=qcol),
                        mapping=dict(zip(col_x, [None for i in range(len(col_x))])),
                        is_descending_sorted=is_descending_sorted,
                    )
                chart_sub_table[row[x_col_idx]] = datetime_to_linear_ticks(row[qcol.idx]) if qcol.is_datetime else row[qcol.idx]
        self.chart_sub_tables = list(chart_sub_tables_dict.values())

        col_y_min = properties.get(VisualizationKeys.Y_MIN)
        col_y_min = col_y_min if is_quantity(col_y_min) else None

        col_y_max = properties.get(VisualizationKeys.Y_MAX)
        col_y_max = col_y_max if is_quantity(col_y_max) else None

        for tab in self.chart_sub_tables:
            tab.col_y_min = col_y_min
            tab.col_y_max = col_y_max
            if tab.col_y_min is None and tab.col_y_max is None:
                pass
            elif tab.col_y_min is not None and tab.col_y_max is not None:
                pass
            elif tab.col_y_min is None:
                try:
                    tab.col_y_min = min(min(filter(lambda x: x is not None, tab.values())), 0) * 1.1
                except:
                    tab.col_y_min = None
            elif tab.col_y_max is None:
                try:
                    tab.col_y_max = max(filter(lambda x: x is not None, tab.values())) * 1.1
                except:
                    tab.col_y_max = None

        return self.chart_sub_tables


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


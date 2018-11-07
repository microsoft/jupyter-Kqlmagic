# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

""" Constants file. """


class Constants(object):
    MINIMAL_PYTHON_VERSION_REQUIRED = "3.6"
    MAGIC_SOURCE_REPOSITORY_NAME = "https://github.com/Microsoft/jupyter-Kqlmagic"
    MAGIC_PIP_REFERENCE_NAME = "Kqlmagic"  # 'git+git://github.com/Microsoft/jupyter-Kqlmagic.git'
    MAGIC_PACKAGE_NAME = "Kqlmagic"
    
    # class must be uppercase, because %config can't find it if it is lowercase?
    MAGIC_CLASS_NAME = "Kqlmagic"
    MAGIC_NAME = "kql"
    MAGIC_ALIASES = []
    LOGGER_NAME = "{0}-py".format(MAGIC_CLASS_NAME)


class ConnStrKeys(object):
    # make sure all keys are lowercase, without spaces, underscores, and hyphen-minus
    # because they are ignored
    TENANT = "tenant"
    USERNAME = "username"
    PASSWORD = "password"
    CLIENTID = "clientid"
    CLIENTSECRET = "clientsecret"
    CERTIFICATE = "certificate"
    CERTIFICATE_PEM_FILE = "certificatepemfile"
    CERTIFICATE_THUMBPRINT = "certificatethumbprint"
    APPKEY = "appkey"
    CODE = "code"

    CLUSTER = "cluster"
    DATABASE = "database"
    WORKSPACE = "workspace"
    APPID = "appid"
    FOLDER = "folder"


    ALIAS = "alias"

class VisualizationKeys(object):
    "list of keys as they appear in response from kusto or draft"
    VISUALIZATION = "Visualization"
    TITLE = "Title"
    X_COLUMN = "XColumn"
    SERIES = "Series"
    Y_COLUMN = "YColumns"
    ANOMALY_COLUMNS = "AnomalyColumns"
    X_TITLE = "XTitle"
    Y_TITLE = "YTitle"
    X_AXIX = "XAxis"
    Y_AXIX = "YAxis"
    LEGEND = "Legend"
    Y_SPLIT = "YSplit"
    ACCUMULATE = "Accumulate"
    IS_QUERY_SORTED = "IsQuerySorted"
    KIND = "Kind"

class VisualizationValues(object):
    TABLE = "table"
    PIE_CHART = "piechart"
    BAR_CHART = "barchart"
    COLUMN_CHART = "columnchart"
    AREA_CHART = "areachart"
    LINE_CHART = "linechart"
    TIME_CHART = "timechart"
    ANOMALY_CHART = "anomalychart"
    STACKED_AREA_CHART = "stackedareachart"
    LADDER_CHART = "ladderchart"
    TIME_PIVOT = "timepivot"
    PIVOT_CHART = "pivotchart"
    SCATTER_CHART = "scatterchart"


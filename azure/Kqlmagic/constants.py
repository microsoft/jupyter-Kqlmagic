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
    LOGGER_NAME = f"{MAGIC_CLASS_NAME}-py"

    # conversion constants
    MINUTE_SECS        =                           60
    HOUR_SECS          =             60 * MINUTE_SECS 
    DAY_SECS           =               24 * HOUR_SECS
    SEC_NANOS          =                   1000000000
    TICK_NANOS         =        100 # 1 tick is 100ns
    TICK_TO_INT_FACTOR = int(SEC_NANOS // TICK_NANOS)

    # SSO
    SSO_GC_INTERVAL_IN_SECS = 1 * HOUR_SECS
    SSO_ENV_VAR_NAME  = f"{MAGIC_CLASS_NAME.upper()}_SSO_ENCRYPTION_KEYS"
    SSO_DB_KEY_PREFIX =        f"{MAGIC_CLASS_NAME.lower()}store/tokens/"

class Schema(object):
    APPLICATION_INSIGHTS = "applicationinsights"
    LOG_ANALYTICS        =        "loganalytics"
    AZURE_DATA_EXPLORER  =   "azuredataexplorer" 

class Cloud(object):
    PUBLIC      =      "public"
    MOONCAKE    =    "mooncake"
    FAIRFAX     =     "fairfax"
    BLACKFOREST = "blackforest"
    USNAT       =       "usnat"
    USSEC       =       "ussec"
    TEST        =        "test"

class ConnStrKeys(object):
    # make sure all keys are lowercase, without spaces, underscores, and hyphen-minus
    # because they are ignored
    TENANT                 =                "tenant"
    USERNAME               =              "username"
    PASSWORD               =              "password"
    CLIENTID               =              "clientid"
    CLIENTSECRET           =          "clientsecret"
    CERTIFICATE            =           "certificate"
    CERTIFICATE_PEM_FILE   =    "certificatepemfile"
    CERTIFICATE_THUMBPRINT = "certificatethumbprint"
    APPKEY                 =                "appkey"
    CODE                   =                  "code"
    ANONYMOUS              =             "anonymous"
    CLUSTER                =               "cluster"
    DATABASE               =              "database"
    WORKSPACE              =             "workspace"
    APPID                  =                 "appid"
    FOLDER                 =                "folder"
    AAD_URL                =                "aadurl"
    DATA_SOURCE_URL        =         "datasourceurl"
    ALIAS                  =                 "alias"

class ConnCombinationProperty(object):
    REQUIRED = "required"
    EXTRA    =    "extra"
    OPTIONAL = "optional"

class VisualizationKeys(object):
    "list of keys as they appear in response from kusto or draft"

    VISUALIZATION = "Visualization"
    """Visualization indicates the kind of visualization to use. The supported values are:
        anomalychart	Similar to timechart, but highlights anomalies using an external machine-learning service.
        areachart	    Area graph. First column is x-axis, and should be a numeric column. Other numeric columns are y-axes.
        barchart	    First column is x-axis, and can be text, datetime or numeric. Other columns are numeric, displayed as horizontal strips.
        columnchart	    Like barchart, with vertical strips instead of horizontal strips.
        ladderchart	    Last two columns are the x-axis, other columns are y-axis.
        linechart	    Line graph. First column is x-axis, and should be a numeric column. Other numeric columns are y-axes.
        piechart	    First column is color-axis, second column is numeric.
        pivotchart	    Displays a pivot table and chart. User can interactively select data, columns, rows and various chart types.
        scatterchart	Points graph. First column is x-axis, and should be a numeric column. Other numeric columns are y-axes.
        stackedareachart	Stacked area graph. First column is x-axis, and should be a numeric column. Other numeric columns are y-axes.
        table	        Default - results are shown as a table.
        timechart	    Line graph. First column is x-axis, and should be datetime. Other columns are y-axes.
        timepivot	    Interactive navigation over the events time-line (pivoting on time axis)    
    """

    TITLE = "Title"
    "The title of the visualization (of type string)."

    X_TITLE = "XTitle"
    "The title of the x-axis (of type string)."

    Y_TITLE = "YTitle"
    "The title of the y-axis (of type string)."

    X_COLUMN = "XColumn"
    "Which column in the result is used for the x-axis."

    SERIES = "Series"
    "Comma-delimited list of columns whose combined per-record values define the series that record belongs to."

    Y_COLUMNS = "YColumns"
    "Comma-delimited list of columns that consist of the values provided per value of the x column."

    X_AXIS = "XAxis"
    "How to scale the x-axis (linear or log)."

    Y_AXIS = "YAxis"
    "How to scale the y-axis (linear or log)."
    
    LEGEND = "Legend"
    "Whether to display a legend or not (visible or hidden)."

    # TODO: implement it
    Y_SPLIT = "YSplit"
    """How to split multiple the visualization.

       Some visualizations support splitting into multiple y-axis values:
            none	A single y-axis is displayed for all series data. (Default)
            axes	A single chart is displayed with multiple y-axis (one per series).
            panels	One chart is rendered for each ycolumn value (up to some limit).
    """

    # TODO: implement it
    ACCUMULATE = "Accumulate"
    "Whether the value of each measure gets added to all its predecessors. (true or false)"

    IS_QUERY_SORTED = "IsQuerySorted"
    "Tips: Sort the data to define the order of the x-axis."

    # TODO: implement it
    KIND = "Kind"
    """Some visualizations can be further elaborated by providing the kind property.
    
    Visualization	kind	        Description

    areachart	    default	        Each "area" stands on its own.
                    unstacked	    Same as default.
                    stacked	        Stack "areas" to the right.
                    stacked100	    Stack "areas" to the right and stretch each one to the same width as the others.
    barchart	    default	        Each "bar" stands on its own.
                    unstacked	    Same as default.
                    stacked	        Stack "bars".
                    stacked100	    Stack "bard" and stretch each one to the same width as the others.
    columnchart	    default	        Each "column" stands on its own.
                    unstacked	    Same as default.
                    stacked	        Stack "columns" one atop the other.
                    stacked100	    Stack "columns" and stretch each one to the same height as the others.
    """
   
    # TODO: find out what it means
    ANOMALY_COLUMNS = "AnomalyColumns"
 
class VisualizationValues(object):
    TABLE              =            "table"
    PIE_CHART          =         "piechart"
    BAR_CHART          =         "barchart"
    COLUMN_CHART       =      "columnchart"
    AREA_CHART         =        "areachart"
    LINE_CHART         =        "linechart"
    TIME_CHART         =        "timechart"
    ANOMALY_CHART      =     "anomalychart"
    STACKED_AREA_CHART = "stackedareachart"
    LADDER_CHART       =      "ladderchart"
    TIME_PIVOT         =        "timepivot"
    PIVOT_CHART        =       "pivotchart"
    SCATTER_CHART      =     "scatterchart"

class VisualizationKinds(object):
    DEFAULT = "default"
    "Each y-value stands on its own."

    UNSTACKED = "unstacked"
    "Same as default, each y-value stands on its own."

    STACKED = "stacked"
    "Stack y-values one atop the other."

    STACKED_100 = "stacked100"
    "Stack y-values and stretch each one to the same height as the others."

class VisualizationSplits(object):
    NONE = "none"
    "A single y-axis is displayed for all series data. (Default)"

    AXES = "axes"
    "A single chart is displayed with multiple y-axis (one per series)."

    PANELS = "panels"
    "One chart is rendered for each ycolumn value (up to some limit)."

class VisualizationScales(object):
    LINEAR = "linear"
    LOG    =    "log"

class VisualizationLegends(object):
    HIDDEN  =  "hidden"
    VISIBLE = "visible"

# SSO Constants
class CryptoParam(object):
    PASSWORD   =   "password"
    SALT       =       "salt"
    LENGTH     =     "length"
    ITERATIONS = "iterations"
    ALGORITHM  =  "algorithm"
    BACKEND    =    "backend"
    ENCRYPT_KEY = "encryptionkey"

class SsoStorageParam(object):
    AUTHORITY      =      "authority"
    CRYPTO_OBJ     =     "crypto_obj"
    CACHE_NAME     =     "cache_name"
    GC_TTL_IN_SECS = "gc_ttl_in_secs"

class SsoEnvVarParam(object):
    CACHE_NAME       =      "cachename"
    SECRET_KEY       =      "secretkey"
    SECRET_SALT_UUID = "secretsaltuuid"
    CRYPTO           =         "crypto"
    STORAGE          =        "storage"
    ENCRYPT_KEY = "encryptionkey"

class SsoStorage(object):
    IPYTHON_DB = "ipythondb"

class SsoCrypto(object):
    DPAPI  =  "dpapi"
    FERNET = "fernet"

class DpapiParam(object):
    DESCRIPTION = "description"
    SALT        =        "salt"




# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------


""" Constants file. """


class Constants(object):
    IPYKERNEL_CELL_MAGIC_PREFIX ="%%"
    IPYKERNEL_LINE_MAGIC_PREFIX ="%"
    MINIMAL_PYTHON_VERSION_REQUIRED = "3.6"
    MAGIC_SOURCE_REPOSITORY_NAME = "https://github.com/Microsoft/jupyter-Kqlmagic"
    MAGIC_PIP_REFERENCE_NAME = "Kqlmagic"  # 'git+git://github.com/Microsoft/jupyter-Kqlmagic.git'
    MAGIC_PACKAGE_NAME = "Kqlmagic"
    MAGIC_ISSUES_REPOSITORY_URL = "https://github.com/microsoft/jupyter-Kqlmagic/issues/new"
    
    
    # class must start with an uppercase, because %config can't find it if it is lowercase?
    MAGIC_CLASS_NAME = "Kqlmagic"
    MAGIC_CLASS_NAME_UPPER = MAGIC_CLASS_NAME.upper()
    MAGIC_CLASS_NAME_LOWER = MAGIC_CLASS_NAME.lower()
    MAGIC_NAME = "kql"
    CELL_MAGIC_PREFIX = f"{IPYKERNEL_CELL_MAGIC_PREFIX}{MAGIC_NAME}"
    LINE_MAGIC_PREFIX = f"{IPYKERNEL_LINE_MAGIC_PREFIX}{MAGIC_NAME}"
    MAGIC_ALIASES = []
    LOGGER_NAME = f"{MAGIC_CLASS_NAME}-py"

    DONT_ADD_CELL_MAGIC_PREFIX = f"#dont_add_{MAGIC_CLASS_NAME_LOWER}_cell_prefix\n" # MUST NOT INCLUDE whitespaces
    PYTHON_CELL_MAGIC_PREFIX = f"{IPYKERNEL_CELL_MAGIC_PREFIX}py"
    PYTHON_LINE_MAGIC_PREFIX = f"{IPYKERNEL_LINE_MAGIC_PREFIX}py"
    PYTHON_COMMENT_PREFIX = "#"

    # conversion constants
    MINUTE_SECS        =                           60
    HOUR_SECS          =             60 * MINUTE_SECS 
    DAY_SECS           =               24 * HOUR_SECS
    SEC_NANOS          =                   1000000000
    TICK_NANOS         =        100  # 1 tick is 100ns
    TICK_TO_INT_FACTOR = int(SEC_NANOS // TICK_NANOS)

    # SSO
    SSO_GC_INTERVAL_IN_SECS =                                   1 * HOUR_SECS
    SSO_KEYS_ENV_VAR_NAME   = f"{MAGIC_CLASS_NAME_UPPER}_SSO_ENCRYPTION_KEYS"
    SSO_ENV_VAR_NAME        =              f"{MAGIC_CLASS_NAME_UPPER}_SSO_ON"
    SSO_DB_KEY_PREFIX       =       f"{MAGIC_CLASS_NAME_LOWER}_store/tokens/"
    SSO_DEFAULT_CACHE_NAME  =               f"{MAGIC_CLASS_NAME_LOWER}_cache"

    SAW_PYTHON_BRANCH_SUFFIX = "/msft-spython"
     
class PrintColor:
    PURPLE = '\033[95m'
    CYAN = '\033[96m'
    DARKCYAN = '\033[36m'
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    END = '\033[0m'


class Email(object):
    SMTP_PORT          =         "smtpport"
    SMTP_ENDPOINT      =     "smtpendpoint"
    SEND_FROM          =         "sendfrom"
    SEND_TO            =           "sendto"
    SEND_FROM_PASSWORD = "sendfrompassword"
    CONTEXT            =          "context"


class Schema(object):
    APPLICATION_INSIGHTS = "applicationinsights"
    LOG_ANALYTICS        =        "loganalytics"
    AZURE_DATA_EXPLORER  =   "azuredataexplorer"
    AIMON                =               "aimon"
    ARIA                 =                "aria"


class Cloud(object):
    PUBLIC      =      "public"
    MOONCAKE    =    "mooncake"
    CHINA       =       "china"
    FAIRFAX     =     "fairfax"
    GOVERNMENT  =  "government"
    BLACKFOREST = "blackforest"
    GERMANY     =     "germany"
    PPE         =         "ppe"


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

    # internal
    CLUSTER_FRIENDLY_NAME  = "cluster_friendly_name"


class ConnCombinationProperty(object):
    REQUIRED = "required"
    EXTRA    =    "extra"
    OPTIONAL = "optional"


class ExtendedPropertiesKeys(object):
    "list of supported @ExtendedProperties keys that might be included in  response from kusto or draft in @ExtendedProperties table"
    VISUALIZATION = "Visualization"
    CURSOR        =        "Cursor"


class VisualizationKeys(object):
    "list of keys as they appear in response from kusto or draft"

    VISUALIZATION = "Visualization"
    """Visualization indicates the kind of visualization to use. The supported values are:
        anomalychart	Similar to timechart, but highlights anomalies using series_decompose_anomalies function.
        areachart	    Area graph. First column is x-axis, and should be a numeric column. Other numeric columns are y-axes.
        barchart	    First column is the x-axis and can be text, datetime or numeric. Other columns are numeric, displayed as horizontal strips.
        card            First result record is treated as set of scalar values and shows as a card.
        columnchart	    Like barchart with vertical strips instead of horizontal strips.
        ladderchart	    Last two columns are the x-axis, other columns are y-axis.
        linechart	    Line graph. First column is x-axis, and should be a numeric column. Other numeric columns are y-axes.
        piechart	    First column is color-axis, second column is numeric.
        pivotchart	    Displays a pivot table and chart. User can interactively select data, columns, rows and various chart types.
        scatterchart	Points graph. First column is x-axis and should be a numeric column. Other numeric columns are y-axes.
        stackedareachart	Stacked area graph. First column is x-axis, and should be a numeric column. Other numeric columns are y-axes.
        table	        Default - results are shown as a table.
        timechart	    Line graph. First column is x-axis, and should be datetime. Other (numeric) columns are y-axes. 
                        There is one string column whose values are used to "group" the numeric columns and create different lines in the chart 
                        (further string columns are ignored).
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

    Y_MIN = "Ymin"
    "The minimum value to be displayed on Y-axis."

    Y_MAX = "Ymax"
    "The maximum value to be displayed on Y-axis"

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
    piechart        map             Expected columns are [Longitude, Latitude] or GeoJSON point, color-axis and numeric. Supported in Kusto Explorer desktop.
    scatterchart    map             Expected columns are [Longitude, Latitude] or GeoJSON point. Series column is optional. Supported in Kusto Explorer desktop.
    """
   
    # TODO: find out what it means
    ANOMALY_COLUMNS = "AnomalyColumns"
    """Property relevant only for anomalychart. Comma-delimited list of columns which will be considered as anomaly series and displayed as points on the chart"""
 

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
    CRYPTO_KEY = "crypto_key"
    PASSWORD   =   "password"
    SALT       =       "salt"
    LENGTH     =     "length"
    ITERATIONS = "iterations"
    ALGORITHM  =  "algorithm"
    BACKEND    =    "backend"


class SsoStorageParam(object):
    CACHE_SELECTOR_KEY = "cache_selector_key"
    CRYPTO_OBJ         =         "crypto_obj"
    CRYPTO_CLEAR_FUNC  =  "crypto_clear_func"
    CACHE_NAME         =         "cache_name"
    GC_TTL_IN_SECS     =     "gc_ttl_in_secs"


class SsoEnvVarParam(object):
    CACHE_NAME       =      "cachename"
    SECRET_KEY       =      "secretkey"
    SECRET_SALT_UUID = "secretsaltuuid"
    CRYPTO           =         "crypto"
    STORAGE          =        "storage"


class SsoStorage(object):
    IPYTHON_DB      =       "ipythondb"

    DEFAULT         =       "ipythondb"


class SsoCrypto(object):
    DPAPI           =           "dpapi"
    FERNET          =          "fernet"
    LINUX_LIBSECRET = "linux_libsecret"
    OSX_KEYCHAIN    =    "osx_keychain"

    AUTO            =            "auto"
    DEFAULT         =            "auto"


class DpapiParam(object):
    DESCRIPTION = "description"
    ENTROPY     =     "entropy"


class Profile(object):
    app = {

        "nteract": {
            "support_javascript":       True,
            "support_deep_link_script": False,
            "support_auth_script":      False,
                    "support_reconnect_script": False,
            "support_file_url":         False,
                    "support_help_menu":        False,
                    "support_json_object":      True,
        },

        "azuredatastudio": {
            "support_javascript":       True,
            "support_deep_link_script": True,
            "support_auth_script":      True,
                    "support_reconnect_script": False,
            "support_file_url":         True,
                    "support__help_menu":       False,
                    "support_json_object":      False,
        },

        "azuredatastudiosaw": {
            "support_javascript":       True,
            "support_deep_link_script": True,
            "support_auth_script":      True,
                    "support_reconnect_script": False,
            "support_file_url":         True,
                    "support__help_menu":       False,
                    "support_json_object":      False,
        },

        "visualstudiocode": {
            "support_javascript":       True,
            "support_deep_link_script": True,
            "support_auth_script":      True,
                    "support_reconnect_script": False,
            "support_file_url":         True,
                    "support__help_menu":       False,
                    "support_json_object":      False,
        },

        "jupyternotebook": {
            "support_javascript":       True,
            "support_deep_link_script": True,
            "support_auth_script":      True,
                    "support_reconnect_script": True,
            "support_file_url":         True,
                    "support__help_menu":       True,
                    "support_json_object":      False,
        },

        "azurenotebook": {
            "support_javascript":       True,
            "support_deep_link_script": True,
            "support_auth_script":      True,
                    "support_reconnect_script": True,
            "support_file_url":         True,
                    "support__help_menu":       True,
                    "support_json_object":      False,
        },

        "azureml": {
            "support_javascript":       True,
            "support_deep_link_script": True,
            "support_auth_script":      True,
                    "support_reconnect_script": True,
            "support_file_url":         True,
                    "support__help_menu":       True,
                    "support_json_object":      False,
        },

        "azuremljupyternotebook": {
            "support_javascript":       True,
            "support_deep_link_script": True,
            "support_auth_script":      True,
                    "support_reconnect_script": True,
            "support_file_url":         True,
                    "support__help_menu":       True,
                    "support_json_object":      False,
        },

        "azuremljupyterlab": {
            "support_javascript":       True,
            "support_deep_link_script": True,
            "support_auth_script":      True,
                    "support_reconnect_script": False,
            "support_file_url":         True,
                    "support__help_menu":       False,
                    "support_json_object":      True,
        },
        
        "jupyterlab": {
            "support_javascript":       True,
            "support_deep_link_script": True,
            "support_auth_script":      True,
                    "support_reconnect_script": False,
            "support_file_url":         True,
                    "support__help_menu":       False,
                    "support_json_object":      True,
        },

        "ipython": {
            "support_javascript":       True,
            "support_deep_link_script": True,
            "support_auth_script":      True,
                    "support_reconnect_script": False,
            "support_file_url":         True,
                    "support__help_menu":       False,
                    "support_json_object":      False,
        }
    }

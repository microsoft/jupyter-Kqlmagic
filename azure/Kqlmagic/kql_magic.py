# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

import os
import sys
import time
import json
import logging
import hashlib
import urllib.request


from .log import logger

logger().debug("kql_magic.py - import Configurable from traitlets.config.configurable")
from traitlets.config.configurable import Configurable
logger().debug("kql_magic.py - import Bool, Int, Float, Unicode, Enum, TraitError, validate from traitlets")
from traitlets import Bool, Int, Float, Unicode, Enum, TraitError, validate, TraitType

logger().debug("kql_magic.py - import Magics, magics_class, cell_magic, line_magic, needs_local_scope from IPython.core.magic")
try:
    from IPython.core.magic import Magics, magics_class, cell_magic, line_magic, needs_local_scope
except Exception:
    class Magics(object):
        def __init__(self, shell):
            pass
    def magics_class(_class):
        class _magics_class(object):
            def __init__(self, *args, **kwargs):
                self.oInstance = _class(*args,**kwargs)
            def __getattribute__(self,s):
                try:    
                    x = super(_magics_class,self).__getattribute__(s)
                except AttributeError:      
                    return self.oInstance.__getattribute__(s)
                else:
                    return x
        return _magics_class
    def cell_magic(_name):
        def _cell_magic(_func):
            return _func
        return _cell_magic
    def line_magic(_name):
        def _line_magic(_func):
            return _func
        return _line_magic
    def needs_local_scope(_func):
        return _func
    


from .kql_magic_core import Kqlmagic_core
from .constants import Constants, Cloud
from .palette import Palettes, Palette
try:
    from flask import Flask
except Exception:
    flask_installed = False
else:
    flask_installed = True
from .display import Display

from .results import ResultSet

kql_core_obj = None

@magics_class
class Kqlmagic(Magics, Configurable):

    auto_limit = Int(
        0, 
        config=True, 
        allow_none=True, 
        help="""Automatically limit the size of the returned result sets.\n
        Abbreviation: 'al'"""
    )

    prettytable_style = Enum(
        ["DEFAULT", "MSWORD_FRIENDLY", "PLAIN_COLUMNS", "RANDOM"],
        "DEFAULT",
        config=True,
        help="""Set the table printing style to any of prettytable's defined styles.\n
        Abbreviation: 'ptst'"""
    )

    short_errors = Bool(
        True, 
        config=True, 
        help="""Don't display the full traceback on KQL Programming Error.\n
        Abbreviation: 'se'"""
    )

    display_limit = Int(
        None,
        config=True,
        allow_none=True,
        help="""Automatically limit the number of rows displayed (full result set is still stored).\n
        Abbreviation: 'dl'""",
    )

    auto_dataframe = Bool(
        False, 
        config=True, 
        help="""Return Pandas dataframe instead of regular result sets.\n
        Abbreviation: 'ad'"""
    )

    columns_to_local_vars = Bool(
        False, 
        config=True, 
        help="""Return data into local variables from column names.\n
        Abbreviation: 'c2lv'"""
    )

    feedback = Bool(
        True, 
        config=True, 
        help="""Show number of records returned, and assigned variables.\n
        Abbreviation: 'f'"""
    )

    show_conn_info = Enum(
        ["list", "current", "None"],
        "current",
        config=True,
        allow_none=True,
        help="""Show connection info, either current, the whole list, or None.\n
        Abbreviation: 'sci'"""
    )

    dsn_filename = Unicode(
        "odbc.ini",
        config=True,
        allow_none=True,
        help="""Sets path to DSN file.\n
        When the first argument of the connection string is of the form [section], a kql connection string is formed from the matching section in the DSN file.\n
        Abbreviation: 'dl'"""
    )

    cloud = Enum(
        [Cloud.PUBLIC, Cloud.MOONCAKE, Cloud.FAIRFAX, Cloud.BLACKFOREST],
        Cloud.PUBLIC,
        config=True,
        help="""Default cloud\n
        the kql connection will use the cloud as specified"""
    )

    enable_sso = Bool(
        False, 
        config = True, 
        help=f"""Enables or disables SSO.\n
        If enabled, SSO will only work if the environment parameter {Constants.MAGIC_CLASS_NAME.upper()}_SSO_ENCRYPTION_KEYS is set properly."""
    )

    sso_db_gc_interval = Int(
        168, 
        config=True,
        help= """Garbage Collection interval for not changed SSO cache entries. Default is one week."""
    )

    device_code_login_notification = Enum(
        ["frontend", "browser", "terminal", "email"],
        "frontend", 
        config = True, 
        help = """Sets device_code login notification method.\n
        Abbreviation: 'dcln'"""
    )

    device_code_notification_email = Unicode(
        "", 
        config=True, 
        help=f"""Email details. Should be set by {Constants.MAGIC_CLASS_NAME.upper()}_DEVICE_CODE_NOTIFICATION_EMAIL.\n
        the email details string format is: SMTPEndPoint='endpoint';SMTPPort='port';sendFrom='email';sendFromPassword='password';sendTo='email';context='text'\n
        Abbreviation: 'dcne'"""
    )

    timeout = Int(
        None, 
        config=True, 
        allow_none=True, 
        help="""Specifies the maximum time in seconds, to wait for a query response. None, means default http wait time.\n
        Abbreviation: 'to' or 'wait'"""
    )

    plot_package = Enum(
        ["None", "plotly", "plotly_orca", 'plotly_widget'], 
        "plotly", 
        config=True, 
        help="""Set the plot package (plotlt_orca requires plotly orca to be installed on the server).\n 
        Abbreviation: 'pp'"""
    )

    table_package = Enum(
        ["prettytable", "pandas", "plotly", "qgrid"], 
        "prettytable", 
        config=True, 
        help="Set the table display package. Abbreviation: tp"
    )

    last_raw_result_var = Unicode(
        "_kql_raw_result_", 
        config=True, 
        help="""Set the name of the variable that will contain last raw result.\n
        Abbreviation: 'var'"""
    )

    enable_suppress_result = Bool(
        True, 
        config=True, 
        help="""Suppress result when magic ends with a semicolon ;.\n 
        Abbreviation: 'esr'"""
    )

    show_query_time = Bool(
        True, 
        config=True, 
        help="""Print query execution elapsed time.\n 
        Abbreviation: 'sqt'"""
    )

    show_query = Bool(
        False, 
        config=True, 
        help="""Print parametrized query.\n
        Abbreviation: 'sq'"""
    )

    show_query_link = Bool(
        False, 
        config=True, 
        help="""Show query deep link as a button, to run query in the deafult tool.\n
        Abbreviation: ''sql'"""
    )

    query_link_destination = Enum(
        ["Kusto.Explorer", "Kusto.WebExplorer"], 
        "Kusto.WebExplorer", 
        config=True, help="""Set the deep link destination.\n
        Abbreviation: 'qld'"""
    )

    plotly_fs_includejs = Bool(
        False,
        config=True,
        help="""Include plotly javascript code in popup window. If set to False (default), it download the script from https://cdn.plot.ly/plotly-latest.min.js.\n
        Abbreviation: 'pfi'"""
    )

    validate_connection_string = Bool(
        True, 
        config=True, 
        help="Validate connectionString with an implicit query, when query statement is missing. Abbreviation: vc"
    )

    auto_popup_schema = Bool(
        True, 
        config=True, 
        help="""Popup schema when connecting to a new database.\n
        Abbreviation: 'aps'"""
    )

    json_display = Enum(
        ["raw", "native", "formatted"], 
        "formatted", 
        config=True, 
        help="""Set json/dict display format.\n
        Abbreviation: 'jd'"""
    )

    palette_name = Unicode(
        Palettes.DEFAULT_NAME, 
        config=True, 
        help="""Set pallete by name to be used for charts.\n
        Abbreviation: 'pn'"""
    )

    palette_colors = Int(
        Palettes.DEFAULT_N_COLORS, 
        config=True, 
        help="""Set pallete number of colors to be used for charts.\n
        Abbreviation: 'pc'"""
    
    )
    palette_desaturation = Float(
        Palettes.DEFAULT_DESATURATION, 
        config=True, 
        help="""Set pallete desaturation to be used for charts.\n
        Abbreviation: 'pd'"""
    )

    temp_folder_name = Unicode(
        f"{Constants.MAGIC_CLASS_NAME}_temp_files", 
        config=True, 
        help="""Set the folder name for temporary files"""
    )

    export_folder_name = Unicode(
        f"{Constants.MAGIC_CLASS_NAME}_exported_files", 
        config=True, 
        help="""Set the folder name  for exported files"""
    )

    popup_interaction = Enum(
        ["auto", "button", "reference", "webbrowser_open_at_kernel", "reference_popup"],
        "auto",
        config=True, 
        help="""Set popup interaction.\n
        Abbreviation: 'pi'"""        
    )

    temp_files_server = Enum(
        ["auto", "jupyter", "kqlmagic", "disabled"],
        "auto" if flask_installed else "disabled" ,
        config=True, 
        help="""Temp files server."""        
    )

    cache_folder_name = Unicode(
        f"{Constants.MAGIC_CLASS_NAME}_cache_files", 
        config=True, 
        help="Set the folder name for cache files"
    )

    # valid values: jupyterlab or jupyternotebook
    notebook_app = Enum(
        ["auto", "jupyterlab", "jupyternotebook", "ipython", "visualstudiocode", "azuredatastudio"], 
        "auto", 
        config=True, 
        help="""Set notebook application used."""
    ) #TODO: add "papermill", "nteract"

    test_notebook_app = Enum(
        ["none", "jupyterlab", "jupyternotebook", "ipython", "visualstudiocode", "azuredatastudio"], 
        "none", 
        config=True, 
        help="""Set testing application mode, results should return for the specified notebook application."""
    ) #TODO: add "papermill", "nteract"

    add_kql_ref_to_help = Bool(
        True, 
        config=True, 
        help=f"""On {Constants.MAGIC_CLASS_NAME} load, auto add kql reference to Help menu."""
    )

    add_schema_to_help = Bool(
        True, 
        config=True, 
        help="""On connection to database@cluster add  schema to Help menu."""
    )

    cache = Unicode(
        None, 
        config=True, 
        allow_none=True, 
        help="""Cache query results to the specified folder."""
    )

    use_cache = Unicode(
        None, 
        config=True, 
        allow_none=True, 
        help="""Use cached query results from the specified folder, instead of executing the query."""
    )

    check_magic_version = Bool(
        True, 
        config=True, 
        help=f"""On {Constants.MAGIC_CLASS_NAME} load, check whether new version of {Constants.MAGIC_CLASS_NAME} exist"""
    )

    show_what_new = Bool(
        True, 
        config=True, 
        help=f"""On {Constants.MAGIC_CLASS_NAME} load, get history file of {Constants.MAGIC_CLASS_NAME} and show what new button to open it"""
    )

    show_init_banner = Bool(
        True, 
        config=True, 
        help=f"""On {Constants.MAGIC_CLASS_NAME} load, show init banner"""
    )

    request_id_tag = Unicode(
        None, 
        config=True, 
        allow_none=True, 
        help=f"""Tags request 'x-ms-client-request-id' header.\n
        Header pattern: {Constants.MAGIC_CLASS_NAME}.execute;{{tag}};{{guid}}\n
        Abbreviation: 'idtag'"""
    )

    request_app_tag = Unicode(
        None, 
        config=True, 
        allow_none=True, 
        help=f"""Tags request 'x-ms-app' header.\n
        Header pattern: {Constants.MAGIC_CLASS_NAME};{{tag}}\n
        Abbreviation: 'apptag'"""
    )

    request_user_tag = Unicode(
        None, 
        config=True, 
        allow_none=True, 
        help=f"""Tags request 'x-ms-user' header.\n
        Header pattern: {{tag}}\n
        Abbreviation: 'usertag''"""
    )

    logger().debug("Kqlmagic:: - define class code")


    @validate("palette_name")
    def _valid_value_palette_name(self, proposal):
        try:
            Palette.validate_palette_name(proposal["value"])
        except (AttributeError, ValueError) as e:
            message = "The 'palette_name' trait of a {0} instance {1}".format(Constants.MAGIC_CLASS_NAME, str(e))
            raise TraitError(message)
        return proposal["value"]


    @validate("palette_desaturation")
    def _valid_value_palette_desaturation(self, proposal):
        try:
            Palette.validate_palette_desaturation(proposal["value"])
        except (AttributeError, ValueError) as e:
            message = "The 'palette_desaturation' trait of a {0} instance {1}".format(Constants.MAGIC_CLASS_NAME, str(e))
            raise TraitError(message)
        return proposal["value"]


    @validate("palette_colors")
    def _valid_value_palette_color(self, proposal):
        try:
            Palette.validate_palette_colors(proposal["value"])
        except (AttributeError, ValueError) as e:
            message = "The 'palette_color' trait of a {0} instance {1}".format(Constants.MAGIC_CLASS_NAME, str(e))
            raise TraitError(message)
        return proposal["value"]


    @validate("notebook_app")
    def _valid_value_notebook_app(self, proposal):
        try:
            if proposal["value"] == "auto":
                raise ValueError("cannot be set to auto, after instance is loaded")
        except (AttributeError , ValueError) as e:
            message = "The 'notebook_app' trait of a {0} instance {1}".format(Constants.MAGIC_CLASS_NAME, str(e))
            raise TraitError(message)
        return proposal["value"]

        
    @validate("temp_files_server")
    def _valid_value_temp_files_server(self, proposal):
        try:
            if (proposal["value"]) != self.temp_files_server:
                if self.temp_files_server == "disabled":
                    raise ValueError("fetaure is 'disabled', due to missing 'flask' module")
                elif proposal["value"] == "auto":
                    raise ValueError("cannot be set to 'auto', after instance is loaded")
                elif proposal["value"] == "disabled":
                    raise ValueError("cannot be set to 'disabled', it is auto set at magic initialization")
                elif proposal["value"] == "auto":
                    raise ValueError("cannot be set to 'auto', after instance is loaded")
                elif proposal["value"] == "kqlmagic":
                    if self.temp_files_server_manager is not None:
                        self.temp_files_server_manager.startServer()
                        Display.showfiles_url_base_path = self.temp_files_server_manager.files_url
                elif proposal["value"] == "jupyter":
                    if self.temp_files_server_manager is not None:
                        self.temp_files_server_manager.abortServer()
                        Display.showfiles_url_base_path = Display.showfiles_file_base_path
        except (AttributeError , ValueError) as e:
            message = "The 'temp_files_server' trait of a {0} instance {1}".format(Constants.MAGIC_CLASS_NAME, str(e))
            raise TraitError(message)
        return proposal["value"]



    def __init__(self, shell, global_ns=None, local_ns=None, is_magic=True):

        global kql_core_obj
        if kql_core_obj is None:
            Configurable.__init__(self, config=(shell.config if shell is not None else None))
            # Add ourself to the list of module configurable via %config
            if shell is not None:
                shell.configurables.append(self)
        if is_magic:
            Magics.__init__(self, shell=shell)
        else:
            setattr(self, 'show_init_banner', False)
        
        kql_core_obj = kql_core_obj or Kqlmagic_core(global_ns=global_ns, local_ns=local_ns, shell=shell, default_options=self)


    @needs_local_scope
    @line_magic(Constants.MAGIC_NAME)
    @cell_magic(Constants.MAGIC_NAME)
    def execute(self, 
        line:str, 
        cell:str="", 
        local_ns:dict={}, 
        override_vars:dict=None, 
        override_options:dict=None, 
        override_query_properties:dict=None, 
        override_connection:str=None, 
        override_result_set=None):

        result = kql_core_obj.execute(
            line=line, 
            cell=cell, 
            local_ns=local_ns,
            override_vars=override_vars,
            override_options=override_options,
            override_query_properties=override_query_properties,
            override_connection=override_connection,
            override_result_set=override_result_set)

        return result


def kql(text:str='', options:dict=None, query_properties:dict=None, vars:dict=None, conn:str=None, global_ns=None, local_ns=None):
    global kql_core_obj
    shell = None
    if kql_core_obj is None:
        if global_ns is None and local_ns is None:
            if 'IPython' in sys.modules:
                from IPython import get_ipython
                shell = get_ipython()
            else:
                global_ns = globals()
                local_ns = locals()

        Kqlmagic(
            shell=shell, 
            global_ns=global_ns, 
            local_ns=local_ns, 
            is_magic=False)

    return kql_core_obj.execute(
        text, 
        override_vars=vars,
        override_options=options, 
        override_query_properties=query_properties, 
        override_connection=conn)


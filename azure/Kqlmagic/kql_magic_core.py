# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

import os
import sys
import re
import time
import json
import logging
import hashlib
import urllib.request


from .log import logger


from traitlets.config.configurable import Configurable
logger().debug("kql_magic_core.py - import Bool, Int, Float, Unicode, Enum, TraitError, validate from traitlets")
from traitlets import Bool, Int, Float, Unicode, Enum, TraitError, validate
try:
    logger().debug("kql_magic_core.py - import Flask, send_file from flask")
    from flask import Flask, send_file
except Exception:
    logger().debug("kql_magic_core.py - flask module not installed")
    flask_installed = False
else:
    flask_installed = True


from .ipython_api import IPythonAPI
from .sso_storage import get_sso_store
from .version import VERSION, get_pypi_latest_version, compare_version, execute_version_command, validate_required_python_version_running
from .help import execute_usage_command, execute_help_command, execute_faq_command, UrlReference, MarkdownString
from .constants import Constants, Cloud
from .my_utils import adjust_path, adjust_path_to_uri, json_dumps

from .results import ResultSet
from .connection import Connection
from .parser import Parser
from .parameterizer import Parameterizer
from .display import Display
from .database_html import Database_html
from .help_html import Help_html
from .kusto_engine import KustoEngine
from .ai_engine import AppinsightsEngine
from .la_engine import LoganalyticsEngine
from .kql_engine import KqlEngineError
from .palette import Palettes, Palette
from .cache_engine import CacheEngine
from .cache_client import CacheClient
from .kql_response import KqlError
from .my_files_server_management import FilesServerManagement


_ENGINES = [KustoEngine, AppinsightsEngine, LoganalyticsEngine, CacheEngine]


class Kqlmagic_core(object):
    """Runs KQL statement on a repository as specified by a connect string.

    Provides the %%kql magic."""

    # make sure the right python version is used
    validate_required_python_version_running(Constants.MINIMAL_PYTHON_VERSION_REQUIRED)

    logger().debug("Kqlmagic_core::class - define class Kqlmagic_core")


    def execute_cache_command(self, cache_name:str) -> str:
        """ execute the cache command.
        command enables or disables caching, and returns a status string

        Returns
        -------
        str
            A string with the new cache name, if None caching is diabled
        """
        if cache_name is not None and cache_name != "None" and len(cache_name) > 0:
            setattr(self.default_options, "cache", cache_name)
            return MarkdownString(f"{Constants.MAGIC_PACKAGE_NAME} caching to folder **{self.default_options.cache}** was enabled.", title="cache enabled")
        else:
            setattr(self.default_options, "cache", None)
            return MarkdownString(f"{Constants.MAGIC_PACKAGE_NAME} caching was disabled.", title="cache disabled")


    def execute_use_cache_command(self, cache_name:str) -> str:
        """ execute the use_cache command.
        command enables or disables use of cache, and returns a status string

        Returns
        -------
        str
            A string with the cache name, if None cache is diabled
        """
        if cache_name is not None and cache_name != "None" and len(cache_name) > 0:
            setattr(self.default_options, "use_cache", cache_name)
            return MarkdownString(f"{Constants.MAGIC_PACKAGE_NAME} cache in folder **{self.default_options.use_cache}** was enabled.", title="use_cache enabled")
        else:
            setattr(self.default_options, "use_cache", None)
            return MarkdownString(f"{Constants.MAGIC_PACKAGE_NAME} cache was disabled.", title="use_cache disabled")


    def execute_clear_sso_db_command(self):
        sso_storage = get_sso_store()
        sso_storage.clear_db()
        return MarkdownString("sso db was cleared.", title="sso cleared")


    def execute_schema_command(self, connection_string:str, user_ns:dict, **options) -> dict:
        """ execute the schema command.
        command return the schema of the connection in json format, so that it can be used programattically

        Returns
        -------
        str
            A string with the cache name, if None cache is diabled
        """

        # removing query and con is required to avoid multiple parameter values error
        _options = {**options}
        _options.pop("query", None)
        connection_string = _options.pop("conn", None) or connection_string
        connection_string = None if connection_string == "None" else connection_string

        conn = Connection.get_connection(connection_string, user_ns, **_options)

        if _options.get("popup_window"):
            schema_file_path  = Database_html.get_schema_file_path(conn, **_options)
            conn_name = conn.kql_engine.get_conn_name() if isinstance(conn, CacheEngine) else conn.get_conn_name()
            button_text = f"popup schema {conn_name}"
            window_name = f"_{conn_name.replace('@', '_at_')}_schema"
            html_obj = Display.get_show_window_html_obj(window_name, schema_file_path, button_text=button_text, onclick_visibility="visible", content="schema", options=_options)
            return html_obj
        else:
            schema_tree = Database_html.get_schema_tree(conn, **_options)
            return Display.to_json_styled_class(schema_tree, style=options.get("schema_json_display"), options=_options)

    # [KUSTO]
    # Driver          = Easysoft ODBC-SQL Server
    # Server          = my_machine\SQLEXPRESS
    # User            = my_domain\my_user
    # Password        = my_password
    # If the database you want to connect to is the default
    # for the SQL Server login, omit this attribute
    # Database        = Northwind


    # Object constructor
    def __init__(self, default_options=None, shell=None, global_ns:dict={}, local_ns:dict={}, config=None, dont_start=False):

        self.default_options = default_options
        self.shell = shell
        self.global_ns = global_ns
        self.local_ns = local_ns
        self.shell_user_ns = self.shell.user_ns if self.shell is not None else self.global_ns
        self.last_raw_result = None
        self.temp_files_server_manager = None
        self.is_temp_files_server_on = None
        if not dont_start:
            self._start()


    def _start(self):


        logger().debug("Kqlmagic_core::_start - start")

        logger().debug("Kqlmagic_core::_start - init options")
        options = self._init_options()

        logger().debug("Kqlmagic_core::_start - set temp folder")
        self._set_temp_files_folder(**options)

        logger().debug("Kqlmagic_core::_start - add kql page reference to jupyter help")
        self._add_kql_ref_to_help(**options)
        logger().debug("Kqlmagic_core::_start - add help items to jupyter help")
        self._add_help_to_jupyter_help_menu(None, start_time=time.time(), options=options)

        logger().debug("Kqlmagic_core::_start - show banner")
        if options.get("show_init_banner"):
            self._show_banner(options)

        logger().debug("Kqlmagic_core::_start - set default connection")
        self._set_default_connections(**options)
        logger().debug("Kqlmagic_core::_start - end")


    def _set_temp_files_folder(self, **options):

        folder_name = options.get("temp_folder_name")
        if folder_name is not None:
            folder_name = f"{Constants.MAGIC_CLASS_NAME_LOWER}/{folder_name}"
            if options.get("temp_folder_location") == "user_dir":
                # app that has a free/tree build server, are not supporting directories athat starts with a dot
                folder_name = f".{folder_name}"

            root_path = IPythonAPI.get_ipython_root_path(**options)
            # nteract
            # root_path = "C:\\Users\\michabin\\Desktop\\nteract-notebooks"
            #
            # add subfolder per kernel_id
            kernel_id = options.get("kernel_id")
            folder_name = f"{folder_name}/{kernel_id}"

            showfiles_folder_Full_name = adjust_path(f"{root_path}/{folder_name}") #dont remove spaces from root directory
            logger().debug(f"Kqlmagic_core::_set_temp_files_folder - showfiles_folder_Full_name: {showfiles_folder_Full_name}")
            if not os.path.exists(showfiles_folder_Full_name):

                logger().debug(f"Kqlmagic_core::_set_temp_files_folder - makedir({showfiles_folder_Full_name})")
                os.makedirs(showfiles_folder_Full_name)

            # kernel will remove folder at shutdown or by restart
            IPythonAPI.try_add_to_ipython_tempdirs(showfiles_folder_Full_name, **options)

            Display.showfiles_file_base_path = adjust_path_to_uri(root_path)
            Display.showfiles_url_base_path = Display.showfiles_file_base_path
            Display.showfiles_folder_name = adjust_path_to_uri(folder_name)


    def _init_options(self):
        self._override_default_configuration()

        _kernel_location = None

        #
        # first we do auto detection
        #
        app = "auto"
        if app == "auto": # ELECTRON_RUN_AS_NODE, MPLBACKEND
            notebook_service_address = getattr(self.default_options, "notebook_service_address")
            if notebook_service_address is not None:
                app = "azurenotebook"
                _kernel_location = "remote"
            elif os.getenv("ADS_LOGS") is not None and os.getenv("ADS_LOGS").find('azuredatastudio') >= 0:
                app = "azuredatastudio"
                _kernel_location = "local"
            elif os.getenv("VSCODE_NODE_CACHED_DATA_DIR") is not None:
                _kernel_location = "local"
                if os.getenv("VSCODE_NODE_CACHED_DATA_DIR").find('azuredatastudio') >= 0:
                    app = "azuredatastudio"
                elif os.getenv("VSCODE_NODE_CACHED_DATA_DIR").find('Code') >= 0:
                    app = "visualstudiocode"
            elif not os.getenv("JPY_PARENT_PID"):
                # _kernel_location = "local"
                # app = "nteract"
                pass

            if app == "auto":
                app_info = self.get_app_from_parent()
                app = app_info.get("app", app)
                _kernel_location = _kernel_location or app_info.get("kernel_location")

            if app == "auto" and not IPythonAPI.has_ipython_kernel():
                _kernel_location = "local"
                app = "ipython"

            if app == "auto":
                app = "jupyternotebook"

            default_app = getattr(self.default_options, "notebook_app", "auto")
            if default_app != "auto" and default_app != app:
                app = default_app
                _kernel_location = None

            setattr(self.default_options, "notebook_app", app)
            logger().debug(f"Kqlmagic_core::_init_options - set default option 'notebook_app' to: {app}")
            # print(f">>> notebook_app: {app}")

        kernel_location = getattr(self.default_options, "kernel_location", "auto")    
        if kernel_location == "auto":
            kernel_location = _kernel_location or "auto"
            if kernel_location == "auto":
                app_info = self.get_app_from_parent()
                kernel_location = app_info.get("kernel_location", kernel_location)
            if kernel_location == "auto":           
                if app in ["azurenotebook"]:
                    kernel_location = "remote"
                elif app in ["ipython"]:
                    kernel_location = "local"
                elif app in ["visualstudiocode", "azuredatastudio", "nteract"]:
                    temp_files_server = getattr(self.default_options, "temp_files_server")
                    kernel_location = "auto" if temp_files_server in ["auto", "kqlmagic"] else "remote"

                # it can stay "auto", maybe based on NOTEBOOK_URL it will be discovered

            setattr(self.default_options, "kernel_location", kernel_location)
            logger().debug(f"Kqlmagic_core::_init_options - set default option 'kernel_location' to: {kernel_location}")
            # print(f">>> kernel_location: {kernel_location}")

        table_package = getattr(self.default_options, "table_package", "auto")
        if table_package == "auto":
            if app in ["azuredatastudio", "nteract"]:
                table_package = "pandas_html_table_schema"
            else:
                table_package = "prettytable"
            setattr(self.default_options, "table_package", table_package)
            logger().debug(f"Kqlmagic_core::_init_options - set default option 'table_package' to: {table_package}")

        temp_folder_location = getattr(self.default_options, "temp_folder_location", "auto")
        if temp_folder_location == "auto":
            if kernel_location in ["auto", "remote"]:
                temp_folder_location = "starting_dir"
            elif app in ["azuredatastudio", "nteract", "visualstudiocode"]:
                temp_folder_location = "user_dir"
            elif app in ["azurenotebook", "jupyternotebook", "jupyterlab"]:
                temp_folder_location = "starting_dir"
            else:
                temp_folder_location = "starting_dir"
            setattr(self.default_options, "temp_folder_location", temp_folder_location)
            logger().debug(f"Kqlmagic_core::_init_options - set default option 'temp_folder_location' to: {temp_folder_location}")

        parsed_queries = Parser.parse(f"dummy_query\n", self.default_options, _ENGINES, {})
        options = parsed_queries[0]["options"]

        return options


    def get_app_from_parent(self):
        try:
            import psutil

            parent_id = os.getppid()
            parent_proc = None
            for proc in psutil.process_iter():
                if proc.pid == parent_id:
                    parent_proc = proc
                    break

            if parent_proc is not None:
                name = parent_proc.name().lower()
                if name.startswith("nteract"):
                    return {"app": "nteract", "kernel_location": "local"}
                elif name.startswith("azuredatastudio"):
                    return {"app": "azuredatastudio", "kernel_location": "local"}
                cmdline = parent_proc.cmdline()
                if cmdline is not None and len(cmdline) > 0:
                    for item in cmdline:
                        item = item.lower()
                        if item.endswith(".exe"):
                            if item.endswith("jupyter-notebook.exe"):
                                return {"app": "jupyternotebook"}
                            elif item.endswith("jupyter-lab.exe"):
                                return {"app": "jupyterlab"}
                            elif item.endswith("azuredatastudio.exe"):
                                return {"app": "azuredatastudio", "kernel_location": "local"}
                            elif item.endswith("nteract.exe"):
                                return {"app": "nteract", "kernel_location": "local"}
        except:
            pass

        return {}


    def get_notebook_host_from_running_processes(self):
        found_item = None
        try:
            import psutil

            for proc in psutil.process_iter():
                try:
                    cmdline = proc.cmdline()
                    for item in cmdline:
                        item = item.lower()
                        if item.endswith("notebooks.azure.com"):
                            if item.startswith("https://"):
                                return item
                            elif item.startswith("http://") or found_item is None:
                                found_item = item
                except:
                    pass
        except:
            pass

        return found_item


    def _add_kql_ref_to_help(self, **options):
        # add help k
        if options.get('notebook_app') != "ipython":
            add_kql_ref_to_help = options.get("add_kql_ref_to_help")
            if add_kql_ref_to_help:
                logger().debug("Kqlmagic::__init__ - add kql reference to help menu")
                Help_html.add_menu_item("kql Reference", "http://aka.ms/kdocs", **options)


    def _show_banner(self, options):
        logger().debug("Kqlmagic_core::_show_banner() - show banner header")
        self._show_banner_header(**options)

        Display.showInfoMessage(
            f"""{Constants.MAGIC_PACKAGE_NAME} package is updated frequently. Run '!pip install {Constants.MAGIC_PIP_REFERENCE_NAME} --no-cache-dir --upgrade' to use the latest version.<br>{Constants.MAGIC_PACKAGE_NAME} version: {VERSION}, source: {Constants.MAGIC_SOURCE_REPOSITORY_NAME}"""
        )

        logger().debug("Kqlmagic_core::_show_banner() - show kqlmagic latest version info")
        self._show_magic_latest_version(**options)

        logger().debug("Kqlmagic_core::_show_banner() - show what's new")
        self._show_what_new(options)


    def _show_banner_header(self, **options):

        # old_logo = 'data:image/png;base64,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'
        # olq_logo-blue_kql ="data:image/png;base64,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"

        logo = "data:image/png;base64,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"

        html_str = (
            f"""<html>
            <head>
            <title>{Constants.MAGIC_CLASS_NAME} - banner</title>
            <style>
            .kql-magic-banner {{
                display: flex; 
                background-color: #d9edf7;
            }}
            .kql-magic-banner > div {{
                margin: 10px; 
                padding: 20px; 
                color: #3a87ad; 
                font-size: 13px;
            }}
            </style>
            </head>
            <body>
                <div class='kql-magic-banner'>
                    <div><img src='{logo}'></div>
                    <div>
                        <p>Kql Query Language, aka kql, is the query language for advanced analytics on Azure Monitor resources. The current supported data sources are 
                        Azure Data Explorer (Kusto), Log Analytics and Application Insights. To get more information execute '%kql --help "kql"'</p>
                        <p>   
                          &bull; 
                kql reference: Click on 'Help' tab > and Select 'kql reference' or execute '%kql --help "kql"'<br>
                          &bull; 
                {Constants.MAGIC_CLASS_NAME} configuration: execute '%config {Constants.MAGIC_CLASS_NAME}'<br>
                          &bull; 
                {Constants.MAGIC_CLASS_NAME} usage: execute '%kql --usage'<br>
                        </p> 
                    </div>
                </div>
            </body>
            </html>"""
            )
        Display.show_html(html_str)


    def _show_magic_latest_version(self, **options):
        if options.get("check_magic_version"):
            try:
                logger().debug("Kqlmagic_core::_show_magic_latest_version - fetch PyPi Kqlmagic latest version")
                only_stable_version = True
                pypi_version = get_pypi_latest_version(Constants.MAGIC_PACKAGE_NAME, only_stable_version)
                ignore_current_version_post = True
                if pypi_version and compare_version(pypi_version, VERSION, ignore_current_version_post) > 0:
                    Display.showWarningMessage(
                        f"""You are using {Constants.MAGIC_PACKAGE_NAME} version {VERSION}, however version {pypi_version} is available. You should consider upgrading, execute '!pip install {Constants.MAGIC_PACKAGE_NAME} --no-cache-dir --upgrade'. To see what's new click on the button below."""
                    )
            except:
                logger().debug("Kqlmagic_core::_show_magic_latest_version - failed to fetch PyPi Kqlmagic latest version")
                pass


    def _show_what_new(self, options):
        if options.get("show_what_new"):
            try:
                logger().debug("Kqlmagic_core::_show_what_new - fetch HISTORY.md")
                # What's new (history.md)  button #
                url = "https://raw.githubusercontent.com/microsoft/jupyter-Kqlmagic/master/HISTORY.md"
                data = urllib.request.urlopen(url)
                data_decoded = data.read().decode('utf-8')
                data_as_markdown = MarkdownString(data_decoded, title=f"what's new")
                html_str  = data_as_markdown._repr_html_()

                if html_str is not None:
                    self._set_temp_files_server(options=options)
                    button_text = "What's New? "
                    file_name = "what_new_history"
                    file_path = Display._html_to_file_path(html_str, file_name, **options)

                    Display.show_window(
                        file_name, 
                        file_path, 
                        button_text=button_text, 
                        onclick_visibility="visible", 
                        before_text=f"Click to find out what's new in {Constants.MAGIC_PACKAGE_NAME} ", 
                        palette=Display.info_style, 
                        **options
                    )
            except:
                logger().debug("Kqlmagic_core::_show_what_new - failed to fetch HISTORY.md")
                pass


    def _set_temp_files_server(self, options={}):
        if (options.get("temp_files_server") == "kqlmagic"
            or (options.get('notebook_app') in ["visualstudiocode", "azuredatastudio", "nteract"]
                and options.get("kernel_location") == "local"
                and options.get("temp_files_server") == "auto")):
            if self.is_temp_files_server_on is None:
                self._start_temp_files_server(options=options)
        elif self.is_temp_files_server_on == True:
            self._abort_temp_files_server(options)


    def _start_temp_files_server(self, options={}):
        self.is_temp_files_server_on = False
        if self.temp_files_server_manager is None and options.get("temp_folder_name") is not None:
            server_py_path = os.getenv(f"{Constants.MAGIC_CLASS_NAME_UPPER}_TEMP_FILES_SERVER_PY_FOLDER")
            if server_py_path is None:
                import Kqlmagic as kqlmagic
                server_py_path = os.path.dirname(kqlmagic.__file__)
            server_py_code = os.path.join(server_py_path, "my_files_server.py")
            SERVER_URL = None
            base_path = Display.showfiles_file_base_path
            folder_name = Display.showfiles_folder_name
            self.temp_files_server_manager = FilesServerManagement(server_py_code, SERVER_URL, adjust_path(f"{base_path}"), folder_name, options=options)
            self.temp_files_server_manager.startServer()
            count = 0
            while not self.temp_files_server_manager.pingServer():
                time.sleep(1)
                count += 1
                if count > 10:
                    break
            if count < 10:
                self.is_temp_files_server_on = True
                Display.showfiles_url_base_path = self.temp_files_server_manager.files_url
                server_url = self.temp_files_server_manager.server_url
                setattr(self.default_options, "temp_files_server_address", server_url)
                options["temp_files_server_address"] = server_url
            else:
                logger().error(f"Kqlmagic_core::::_start_temp_files_server failed to ping server: {self.temp_files_server_manager.server_url}")


    def _abort_temp_files_server(self, options):
        self.is_temp_files_server_on = None
        if self.temp_files_server_manager is not None:
            self.temp_files_server_manager.abortServer()
            self.temp_files_server_manager = None
            setattr(self.default_options, "temp_files_server_address", None)
            logger().debug(f"Kqlmagic_core::_abort_temp_files_server - set defaul option 'temp_files_server_address' to: None")
            options["temp_files_server_address"] = None
            Display.showfiles_url_base_path = Display.showfiles_file_base_path


    def execute(self, line:str, cell:str="", local_ns:dict={},
        override_vars:dict=None, override_options:dict=None, override_query_properties:dict=None, override_connection:str=None, override_result_set=None):
        """Query Kusto or ApplicationInsights using kusto query language (kql). Repository specified by a connect string.

        Magic Syntax::

            %%kql <connection-string>
            <KQL statement>
            # Note: establish connection and query.

            %%kql <established-connection-reference>
            <KQL statemnt>
            # Note: query using an established connection.

            %%kql
            <KQL statement>
            # Note: query using current established connection.

            %kql <KQL statment>
            # Note: single line query using current established connection.

            %kql <connection-string>
            # Note: established connection only.


        Connection string Syntax::

            kusto://username('<username>).password(<password>).cluster(<cluster>).database(<database>')

            appinsights://appid(<appid>).appkey(<appkey>)

            loganalytics://workspace(<workspaceid>).appkey(<appkey>)

            %<connectionStringVariable>%
            # Note: connection string is taken from the environment variable.

            [<sectionName>]
            # Note: connection string is built from the dsn file settings, section <sectionName>. 
            #       The dsn filename value is taken from configuartion value Kqlmagic.dsn_filename.

            # Note: if password or appkey component is missing, user will be prompted.
            # Note: connection string doesn't have to include all components, see examples below.
            # Note: substring of the form $name or ${name} in windows also %name%, are replaced by environment variables if exist.


        Examples::

            %%kql kusto://username('myName').password('myPassword').cluster('myCluster').database('myDatabase')
            <KQL statement>
            # Note: establish connection to Azure Data Explorer (kusto) and submit query.

            %%kql myDatabase@myCluster
            <KQL statement>
            # Note: submit query using using an established kusto connection to myDatabase database at cluster myCluster.

            %%kql appinsights://appid('myAppid').appkey('myAppkey')
            <KQL statement>
            # Note: establish connection to ApplicationInsights and submit query.

            %%kql myAppid@appinsights
            <KQL statement>
            # Note: submit query using established ApplicationInsights connection to myAppid.

            %%kql loganalytics://workspace('myWorkspaceid').appkey('myAppkey')
            <KQL statement>
            # Note: establish connection to LogAnalytics and submit query.

            %%kql myWorkspaceid@loganalytics
            <KQL statement>
            # Note: submit query using established LogAnalytics connection to myWorkspaceid.

            %%kql
            <KQL statement>
            # Note: submit query using current established connection.

            %kql <KQL statement>
            # Note: submit single line query using current established connection.

            %%kql kusto://cluster('myCluster').database('myDatabase')
            <KQL statement>
            # Note: establish connection to kusto using current username and password to form the full connection string and submit query.

            %%kql kusto://database('myDatabase')
            <KQL statement>
            # Note: establish connection to kusto using current username, password and cluster to form the full connection string and submit query.

            %kql kusto://username('myName').password('myPassword').cluster('myCluster')
            # Note: set current (default) username, passsword and cluster to kusto.

            %kql kusto://username('myName').password('myPassword')
            # Note: set current (default) username and password to kusto.

            %kql kusto://cluster('myCluster')
            # Note set current (default) cluster to kusto.
        """

        command = None
        param = None
        logger().debug(f"Kqlmagic_core::execute - input: \n\rline: {line}\n\rcell:\n\r{cell}")
        try:

            if self.shell is not None:
                user_ns = self.shell.user_ns.copy()
                user_ns.update(local_ns)
            else:
                user_ns = self.global_ns.copy()
                user_ns.update(self.local_ns)
                user_ns.update(local_ns)

            parsed = None
            parsed_queries = Parser.parse(f"{line}\n{cell}", self.default_options, _ENGINES, user_ns)
            logger().debug(f"Kqlmagic_core::execute - parsed_queries: {parsed_queries}")
            result = None
            for parsed in parsed_queries:
                parsed["line"] = line
                parsed["cell"] = cell
                popup_text = None

                if type(override_options) is dict:
                    override_options = Parser.validate_override("override_options", self.default_options, **override_options)
                    parsed["options"] = {**parsed["options"], **override_options}
                if type(override_query_properties) is dict:
                    override_query_properties = Parser.validate_override("override_query_properties", self.default_options, **override_query_properties)
                    parsed["options"]["query_properties"] = {**parsed["options"]["query_properties"], **override_query_properties}
                if type(override_connection) is str:
                    parsed["connection"] = override_connection
                options = parsed["options"]
                command = parsed["command"].get("command")

                self._add_help_to_jupyter_help_menu(user_ns, options=options)
                self._set_temp_files_server(options=options)

                if command is None or command == "submit":
                    result = self._execute_query(parsed, user_ns, result_set=override_result_set, override_vars=override_vars)
                    if type(result) == ResultSet:
                        # can't just return result as is, it fails when used with table package pandas_show_schema 
                        # it will first show the result, and will suppres show by result._repr_html
                        result.show_result(suppress_next_repr_html_=True)
                else:
                    param = parsed["command"].get("param")
                    if command == "version":
                        result = execute_version_command()
                    elif command == "banner":
                        self._show_banner(options)
                    elif command == "usage":
                        result = execute_usage_command()
                    elif command == "faq":
                        result = execute_faq_command()
                    elif command == "config":
                        result = self.execute_config_command(param=param, options=options)
                    elif command == "help":
                        result = execute_help_command(param)
                        if result == "config":
                            result = self.execute_config_command(param="None", options=options)
                    elif command == "cache":
                        result = self.execute_cache_command(param)
                    elif command == "use_cache":
                        result = self.execute_use_cache_command(param)
                    elif command == "clear_sso_db":
                        result = self.execute_clear_sso_db_command()
                    elif command == "schema":
                        result = self.execute_schema_command(param, user_ns, **options)
                        # the return is here, because it already handle the popupwindow option case
                        return result
                    elif command == "palette":
                        result = Palette(
                            palette_name=options.get("palette_name"),
                            n_colors=options.get("palette_colors"),
                            desaturation=options.get("palette_desaturation"),
                            to_reverse=options.get("palette_reverse", False),
                        )
                        properties = [
                            options.get("palette_name"), 
                            str(options.get("palette_colors")),
                            str(options.get("palette_desaturation")),
                            str(options.get("palette_reverse", False)),
                        ]
                        param = hashlib.sha1(bytes("#".join(properties), "utf-8")).hexdigest()
                        pname = options.get("palette_name")
                        popup_text = pname if not pname.startswith("[") else "custom"

                    elif command == "palettes":
                        n_colors = options.get("palette_colors")
                        desaturation = options.get("palette_desaturation")
                        result = Palettes(n_colors=n_colors, desaturation=desaturation)
                        properties = [
                            str(options.get("palette_colors")),
                            str(options.get("palette_desaturation")),
                        ]
                        param = hashlib.sha1(bytes("#".join(properties), "utf-8")).hexdigest()
                        popup_text = ' '
                    else:
                        raise ValueError(f"command {command} not implemented")

                    if isinstance(result, UrlReference):
                        file_path = result.url
                        if result.is_raw:
                            data = urllib.request.urlopen(result.url)
                            html_str = data.read().decode('utf-8')
                            if html_str is not None:
                                file_path = Display._html_to_file_path(html_str, result.name, **options)
                        html_obj = Display.get_show_window_html_obj(result.name, file_path, result.button_text, onclick_visibility="visible", options=options)
                        return html_obj

                    if options.get("popup_window"):
                        _repr_html_ = getattr(result, "_repr_html_", None)
                        if _repr_html_ is not None and callable(_repr_html_):
                            html_str = result._repr_html_()
                            if html_str is not None:
                                button_text = f"popup {command} "
                                file_name = f"{command}_command"
                                if param is not None and isinstance(param, str) and len(param) > 0:
                                    file_name += f"_{str(param)}"
                                    popup_text = popup_text or str(param)
                                if popup_text:
                                    button_text += f" {popup_text}"
                                file_path = Display._html_to_file_path(html_str, file_name, **options)
                                html_obj = Display.get_show_window_html_obj(file_name, file_path, button_text=button_text, onclick_visibility="visible", options=options)
                                return html_obj

            return result
        except Exception as e:
            logger().debug(f"Kqlmagic_core::execute - failed - command: {command} param: {param}")
            if parsed:
                if parsed["options"].get("short_errors"):
                    Display.showDangerMessage(str(e))
                    return None
            elif self.default_options.short_errors:
                Display.showDangerMessage(str(e))
                return None
            raise 

    def execute_config_command(self, param:str=None, options:dict={}):
        logger().debug(f"execute_config_command - param: {param}")
        if param == "None":
            from io import StringIO
            c = self.default_options
            help_all = []
            for name, trait in c.class_traits().items():
                if c.trait_metadata(name, "config"):
                    help_parts = c.class_get_trait_help(trait).split("\n")
                    value = getattr(self.default_options, name)
                    if type(value) == str:
                        value = f"'{value}'"
                    help_parts[0] = f"{help_parts[0].replace(Constants.MAGIC_CLASS_NAME + '.', '')}: {value}"
                    "".startswith("    Choices: [")
                    descIdx = 2
                    if help_parts[descIdx].startswith("    Choices: ["):
                        descIdx = 3
                    help_parts[descIdx] = f"    Description: {help_parts[descIdx][4:]}"

                    help = "\n".join(help_parts)
                    # help = c.class_get_trait_help(trait)
                    # help += f"\n    Value: {getattr(self.default_options, name)}"
                    help_all.append(help)

            old_stdout = sys.stdout
            sys.stdout = mystdout = StringIO()
            # this print is not for debug
            print("\n\n".join(help_all))
            sys.stdout = old_stdout
            mystdout.getvalue()

            # added 4 blanks in begining of each line, to maked md formatter keep format as is
            return MarkdownString("    " + mystdout.getvalue().replace("\n","\n    "), title="config")

        if param == "":
            message = f"{Constants.MAGIC_PACKAGE_NAME} doesn't have '' option"
            Display.showDangerMessage(message)
            return None

        kv = param.split(sep='=', maxsplit=1)
        key, value = Parser.parse_option_key("default_configuration", kv[0], config=self.default_options) or (None, None)
        if key is not None:
            if len(kv) == 1:
                return value
            else:
                try:
                    key, value = Parser.parse_option("default_configuration", key, kv[1], config=self.default_options)
                    setattr(self.default_options, key, value)
                    logger().debug(f"execute_config_command - set default option '{key}' to: {value}")
                    options[key] = value
                except Exception as e:
                    if options.get("short_errors"):
                        Display.showDangerMessage(e)
                        return None
                    else:
                        raise e
        else:
            message = f"{Constants.MAGIC_PACKAGE_NAME} doesn't have '{kv[0]}' option"
            Display.showDangerMessage(message)
        

    def _get_connection_info(self, **options):
        mode = options.get("show_conn_info")
        if mode == "current":
            return Connection.get_connection_list_formatted()
        elif mode == "list":
            return [Connection.get_current_connection_formatted()]
        return []


    def _show_connection_info(self, **options):
        msg = self._get_connection_info(**options)
        if len(msg) > 0:
            Display.showInfoMessage(msg)


    def _add_help_to_jupyter_help_menu(self, user_ns, start_time=None, options:dict={}):
        if Help_html.showfiles_base_url is None and self.default_options.notebook_app not in ["azuredatastudio", "ipython", "visualstudiocode", "nteract"]:
            if start_time is not None:
                self._discover_notebook_url_start_time = start_time
            else:
                now_time = time.time()   
                seconds = now_time - self._discover_notebook_url_start_time
                if (seconds < 5):
                    time.sleep(5 - seconds)
                window_location = user_ns.get("NOTEBOOK_URL")
                if window_location is not None:
                    logger().debug(f"_add_help_to_jupyter_help_menu - got NOTEBOOK_URL value: {window_location}")
                    if getattr(self.default_options, "kernel_location") == "auto":
                        if window_location.find("//localhost:") >= 0 or "window_location".find("//127.0.0.") >= 0:
                            kernel_location = "local"
                        else:
                            kernel_location = "remote"
                        setattr(self.default_options, "kernel_location", kernel_location)
                        logger().debug(f"_add_help_to_jupyter_help_menu - set default option 'kernel_location' to: {kernel_location}")
                        options["kernel_location"] = getattr(self.default_options, "kernel_location")
                    Help_html.flush(window_location, options=options)

            if Help_html.showfiles_base_url is None:
                IPythonAPI.try_kernel_execute("""try {IPython.notebook.kernel.execute("NOTEBOOK_URL = '" + window.location + "'");} catch(err) {;}""", **options)


    def _execute_query(self, parsed, user_ns: dict, result_set=None, override_vars=None):

        query = parsed.get('query', '').strip()
        options = parsed.get('options', {})

        suppress_results = options.get("suppress_results", False) and options.get("enable_suppress_result")
        connection_string = parsed.get('connection')

        if not query and not connection_string:
            return None

        try:
            #
            # set connection
            #
            conn = Connection.get_connection(connection_string, user_ns, **options)

        # parse error
        except KqlEngineError as e:
            if options.get("short_errors"):
                msg = "to get help on connection string formats, run: %kql --help 'conn'"
                Display.showDangerMessage(str(e))
                Display.showInfoMessage(msg)
                return None
            else:
                raise

        # parse error
        except ConnectionError as e:
            if options.get("short_errors"):
                Display.showDangerMessage(str(e))
                self._show_connection_info(show_conn_info="list")
                return None
            else:
                raise

        try:
            # validate connection
            if not conn.options.get("validate_connection_string_done") and options.get("validate_connection_string"):
                retry_with_code = False
                try:
                    conn.validate(**options)
                    conn.set_validation_result(True)
                except Exception as e:
                    msg = str(e)
                    if msg.find("AADSTS50079") >= 0 and msg.find("multi-factor authentication") >= 0 and isinstance(conn, KustoEngine):
                        Display.showDangerMessage(str(e))
                        retry_with_code = True
                    else:
                        raise e

                if retry_with_code:
                    Display.showInfoMessage("replaced connection with code authentication")
                    database_name = conn.get_database()
                    cluster_name = conn.get_cluster()
                    uri_schema_name = conn._URI_SCHEMA_NAME
                    # TODO: fix it to have all tokens, but enforce code()
                    connection_string = f"{uri_schema_name}://code().cluster('{cluster_name}').database('{database_name}')"
                    conn = Connection.get_connection(connection_string, user_ns, **options)
                    conn.validate(**options)
                    conn.set_validation_result(True)
                
            conn.options["validate_connection_string_done"] = True

            schema_file_path = None
            if options.get("popup_schema") or (
                not conn.options.get("auto_popup_schema_done") and options.get("auto_popup_schema", self.default_options.auto_popup_schema)
            ):
                schema_file_path = Database_html.get_schema_file_path(conn, **options)
                Database_html.popup_schema(schema_file_path, conn, **options)
            conn.options["auto_popup_schema_done"] = True

            if not conn.options.get("add_schema_to_help_done") and options.get("add_schema_to_help") and options.get("notebook_app") != "ipython":
                schema_file_path = schema_file_path or Database_html.get_schema_file_path(conn, **options)
                Help_html.add_menu_item(conn.get_conn_name(), schema_file_path, **options)
            conn.options["add_schema_to_help_done"] = True

            if not query:
                #
                # If NO  kql query, just return the current connection
                #
                if not connection_string and Connection.connections and not suppress_results:
                    self._show_connection_info(**options)
                return None
            #
            # submit query
            #
            start_time = time.time()
            Parser.validate_query_properties(conn._URI_SCHEMA_NAME, options.get("query_properties"))

            parametrized_query_obj = result_set.parametrized_query_obj if result_set is not None else Parameterizer(query)
            params_vars = parametrized_query_obj.parameters if result_set is not None else options.get("params_dict") or user_ns
            parametrized_query_obj.apply(params_vars, override_vars=override_vars,  **options)
            parametrized_query = parametrized_query_obj.query
            try:
                # print(f">>> parametrized_query: {parametrized_query}")
                raw_query_result = conn.execute(parametrized_query, user_ns, **options)
            except KqlError as err:
                try:
                    parsed_error = json.loads(err.message)
                    message = f"query execution error:\n{json_dumps(parsed_error, indent=4, sort_keys=True)}" 
                except:
                    message = err.message
                Display.showDangerMessage(message)
                return None
            except Exception as e:
                raise e

            end_time = time.time()

            if options.get("save_as") is not None:
                save_as_file_path = CacheClient(**options).save(
                    raw_query_result, conn, parametrized_query, file_path=options.get("save_as"), **options
                )
            if options.get("save_to") is not None:
                save_as_file_path = CacheClient(**options).save(
                    raw_query_result, conn, parametrized_query, filefolder=options.get("save_to"), **options
                )
            #
            # model query results
            #
            conn_info = self._get_connection_info(**options) if not connection_string and Connection.connections else []
            metadata = {
                'magic': self,
                'parsed': parsed,
                'conn': conn,
                'connection': conn.get_conn_name(),
                'start_time': start_time,
                'end_time': end_time,
                'parametrized_query_obj': parametrized_query_obj,
                'conn_info': conn_info
            }
            if result_set is None:
                fork_table_id = 0
                saved_result = ResultSet(metadata, raw_query_result)
            else:
                fork_table_id = result_set.fork_table_id
                saved_result = result_set.fork_result(0)
                saved_result.update_obj(metadata, raw_query_result)
            saved_result.feedback_info = []
            saved_result.feedback_warning = []

            result = saved_result

            if saved_result.is_partial_table:
                saved_result.feedback_warning.append(f"partial results, query had errors (see {options.get('last_raw_result_var')}.dataSetCompletion)")

            if options.get("feedback"):
                if options.get("show_query_time"):
                    minutes, seconds = divmod(end_time - start_time, 60)
                    saved_result.feedback_info.append("Done ({:0>2}:{:06.3f}): {} records".format(int(minutes), seconds, saved_result.records_count))

            if options.get("columns_to_local_vars"):
                # Instead of returning values, set variables directly in the
                # users namespace. Variable names given by column names

                if options.get("feedback"):
                    saved_result.feedback_info.append("Returning raw data to local variables")

                self.shell_user_ns.update(saved_result.to_dict())
                result = None

            if options.get("auto_dataframe"):
                if options.get("feedback"):
                    saved_result.feedback_info.append("Returning data converted to pandas dataframe")
                result = saved_result.to_dataframe()

            if options.get("result_var") and result_set is None:
                result_var = options.get("result_var")
                if options.get("feedback"):
                    saved_result.feedback_info.append(f"Returning data to local variable {result_var}")
                self.shell_user_ns.update({result_var: result if result is not None else saved_result})
                result = None

            if options.get("cache") is not None and options.get("cache") != options.get("use_cache"):
                CacheClient(**options).save(raw_query_result, conn, parametrized_query, **options)
                if options.get("feedback"):
                    saved_result.feedback_info.append("query results cached")

            if options.get("save_as") is not None:
                if options.get("feedback"):
                    saved_result.feedback_info.append(f"query results saved as {save_as_file_path}")
            if options.get("save_to") is not None:
                if options.get("feedback"):
                    path = "/".join(save_as_file_path.split("/")[:-1])
                    saved_result.feedback_info.append(f"query results saved to {path}")

            saved_result.suppress_result = False
            saved_result.display_info = False
            if result is not None:
                if suppress_results:
                    saved_result.suppress_result = True
                elif options.get("auto_dataframe"):
                    Display.showWarningMessage(saved_result.feedback_warning, **options)
                    Display.showSuccessMessage(saved_result.feedback_info, **options)
                else:
                    saved_result.display_info = True

            if result_set is None:
                saved_result._create_fork_results()
            else:
                saved_result._update_fork_results()

            # Return results into the default ipython _ variable
            self.last_raw_result = saved_result
            self.shell_user_ns.update({options.get("last_raw_result_var"): saved_result})


            if isinstance(result, pandas.DataFrame):
                if result.equals(saved_result.to_dataframe()):
                    result = saved_result.fork_result(fork_table_id).to_dataframe()
            elif result == saved_result:
                result = saved_result.fork_result(fork_table_id)

            return result

        except Exception as e:
            if not connection_string and Connection.connections and not suppress_results:
                # display list of all connections
                self._show_connection_info(**options)

            if options.get("short_errors"):
                msg = f"{type(e).__name__}: {e}"
                Display.showDangerMessage(msg)
                return None
            else:
                raise e


    def _override_default_configuration(self):
        f"""override default {Constants.MAGIC_CLASS_NAME} configuration from environment variable {1}_CONFIGURATION.
        the settings should be separated by a semicolon delimiter.
        for example:
        {Constants.MAGIC_CLASS_NAME_UPPER}_CONFIGURATION = 'auto_limit = 1000; auto_dataframe = True' """

        #
        # kernel_id
        #
        kernel_id = IPythonAPI.get_notebook_kernel_id() or "kernel_id"
        setattr(self.default_options, "kernel_id", kernel_id)
        logger().debug(f"_override_default_configuration - set default option 'kernel_id' to: {kernel_id}")

        #
        # KQLMAGIC_NOTEBOOK_SERVICE_ADDRESS
        #
        notebook_service_address = os.getenv(f"{Constants.MAGIC_CLASS_NAME_UPPER}_NOTEBOOK_SERVICE_ADDRESS") or os.getenv("AZURE_NOTEBOOKS_HOST")
        if notebook_service_address is None and (os.getenv("AZURE_NOTEBOOKS_SERVER_URL") is not None or os.getenv("AZURE_NOTEBOOKS_VMVERSION") is not None):
            notebook_service_address = self.get_notebook_host_from_running_processes()
            if notebook_service_address is None or (not notebook_service_address.startswith("https://") and not notebook_service_address.startswith("http://")):
                notebook_service_address = "https://notebooks.azure.com"
        if notebook_service_address is not None:
            setattr(self.default_options, "notebook_service_address", notebook_service_address)
            logger().debug(f"_override_default_configuration - set default option 'notebook_service_address' to: {notebook_service_address}")

        #
        # KQLMAGIC_TEMP_FILES_SERVER
        #
        temp_files_server = os.getenv(f"{Constants.MAGIC_CLASS_NAME_UPPER}_TEMP_FILES_SERVER")
        if temp_files_server is not None:
            setattr(self.default_options, "temp_files_server", temp_files_server)

        #
        # KQLMAGIC_DEVICE_CODE_NOTIFICATION_EMAIL
        #
        email_details = os.getenv(f"{Constants.MAGIC_CLASS_NAME_UPPER}_DEVICE_CODE_NOTIFICATION_EMAIL")
        if email_details is not None:
            email_details = email_details.strip()
            setattr(self.default_options, 'device_code_notification_email', email_details)
            logger().debug(f"_override_default_configuration - set default option 'device_code_notification_email' to: {email_details}")

        #
        # KQLMAGIC_NOTEBOOK_APP
        #
        app = os.getenv(f"{Constants.MAGIC_CLASS_NAME_UPPER}_NOTEBOOK_APP")
        if app is not None:
            lookup_key = app.lower().strip().strip("\"'").replace("_", "").replace("-", "").replace("/", "")
            app = {
                "jupyterlab": "jupyterlab",
                "azurenotebook": "azurenotebook",
                "jupyternotebook": "jupyternotebook", 
                "ipython": "ipython", 
                "visualstudiocode": "visualstudiocode",
                "azuredatastudio": "azuredatastudio",
                "nteract": "nteract",
                "lab": "jupyterlab", 
                "notebook": "jupyternotebook", 
                "ipy": "ipython", 
                "vsc": "visualstudiocode",
                "ads": "azuredatastudio",
                "azure": "azurenotebook",
                "azurenotebooks": "azurenotebook",
                # "papermill":"papermill" #TODO: add "papermill"
            }.get(lookup_key)
            if app is not None:
                app = app.strip()
                setattr(self.default_options, 'notebook_app', app)
                logger().debug(f"_override_default_configuration - set default option 'notebook_app' to: {app}")

        #
        # KQLMAGIC_LOAD_MODE
        #
        load_mode = os.getenv(f"{Constants.MAGIC_CLASS_NAME_UPPER}_LOAD_MODE")
        if load_mode:
            load_mode = load_mode.strip().lower().replace("_", "").replace("-", "")
            if load_mode.startswith("'") or load_mode.startswith('"'):
                load_mode = load_mode[1:-1].strip()
            if load_mode == "silent":
                setattr(self.default_options, 'show_init_banner', False)
                logger().debug(f"_override_default_configuration - set default option 'show_init_banner' to: {False}")

        #
        # KQLMAGIC_CONFIGURATION
        #
        kql_magic_configuration = os.getenv(f"{Constants.MAGIC_CLASS_NAME_UPPER}_CONFIGURATION")
        if kql_magic_configuration is not None:
            kql_magic_configuration = kql_magic_configuration.strip()
            if kql_magic_configuration.startswith("'") or kql_magic_configuration.startswith('"'):
                kql_magic_configuration = kql_magic_configuration[1:-1]

            pairs = kql_magic_configuration.split(";")
            for pair in pairs:
                if pair:
                    kv = pair.split(sep="=", maxsplit=1)
                    key, value = Parser.parse_option("default_configuration", kv[0], kv[1], config=self.default_options)
                    setattr(self.default_options, key, value)
                    logger().debug(f"_override_default_configuration - set default option '{key}' to: {value}")


    def _set_default_connections(self, **options):
        connection_str = os.getenv(f"{Constants.MAGIC_CLASS_NAME_UPPER}_CONNECTION_STR")
        if connection_str is not None:
            connection_str = connection_str.strip()
            if connection_str.startswith("'") or connection_str.startswith('"'):
                connection_str = connection_str[1:-1]
            
            try:
                Connection(connection_str, {}, **options)
                logger().debug(f"_set_default_connections - set default connection_str to: {connection_str}")

            except Exception as err:
                # this print is not for debug
                print(err)

f"""
FAQ

Can I suppress the output of the query?
Answer: Yes you can. Add a semicolumn character ; as the last character of the kql query

Can I submit multiple kql queries in the same cell?
Answer: Yes you can. If you use the line kql magic %kql each line will submit a query. If you use the cell kql magic %%kql you should separate each query by an empty line

Can I save the results of the kql query to a python variable?
Answer: Yes you can. Add a prefix to the query with the variable and '<<'. for example:
        var1 << T | where c > 100

How can I get programmaticaly the last raw results of the last submitted query?
Answer: The raw results of the last submitted query, are save in the object _kql_raw_result_
        (this is the name of the default variable, the variable name can be configured)

Can I submit a kql query that render to a chart?
Answer: Yes you can. The output cell (if not supressed) will show the chart that is specified in the render command

Can I plot the chart of the last query from python?
Answer: Yes you can, assuming the kql query contained a render command. Execute the chart method on the result. for example:
        _kql_raw_result_.show_chart()

Can I display the table of the last query from python?
Answer: Yes you can, assuming the kql query contained a render command. Execute the chart method on the result. for example:
        _kql_raw_result_.show_table()

Can I submit last query again from python?
Answer: Yes you can. Execute the submit method on the result. for example:
        _kql_raw_result_.submit()

Can I get programmaticaly the last query string?
Answer: Yes you can. Get it from the query property of the result. for example:
        _kql_raw_result_.query

Can I get programmaticaly the connection name used for last query?
Answer: Yes you can. Get it from the query property of the result. for example:
        _kql_raw_result_.connection

Can I get programmaticaly the timing metadata of last query?
Answer: Yes you can. Get it from the folowing query properties: start_time, end_time and elapsed_timespan. for example:
        _kql_raw_result_.start_time
        _kql_raw_result_.end_time
        _kql_raw_result_.elapsed_timespan

Can I convert programmaticaly the raw results to a dataframe?
Answer: Yes you can. Execute the to_dataframe method on the result. For example:
        _kql_raw_result_.to_dataframe()

Can I get the kql query results as a dataframe instead of raw data?
Answer: Yes you can. Set the kql magic configuration parameter auto_dataframe to true, and all subsequent queries
        will return a dataframe instead of raw data (_kql_raw_result_ will continue to hold the raw results). For example:
        %config {Constants.MAGIC_CLASS_NAME}.auto_dataframe = True
        %kql var1 << T | where c > 100 // var1 will hold the dataframe

If I use {Constants.MAGIC_CLASS_NAME}.auto_dataframe = True, How can I get programmaticaly the last dataframe results of the last submitted query?
Answer: Execute the to_dataframe method on the result. For example:
        _kql_raw_result_.to_dataframe()

If I use {Constants.MAGIC_CLASS_NAME}.auto_dataframe = True, How can I get programmaticaly the last raw results of the last submitted query?
Answer: _kql_raw_result_ holds the raw results.

"""

#-------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
#--------------------------------------------------------------------------

import os
import time
import logging


from Kqlmagic.version import VERSION, get_pypi_latest_version, compare_version
from Kqlmagic.constants import Constants

logging.getLogger(Constants.LOGGER_NAME).addHandler(logging.NullHandler())

from IPython.core.magic import Magics, magics_class, cell_magic, line_magic, needs_local_scope
from IPython.core.display import display
from IPython.core.magics.display import Javascript


from traitlets.config.configurable import Configurable
from traitlets import Bool, Int, Float, Unicode, Enum, TraitError, validate

from Kqlmagic.connection import Connection
from azure.kusto.data.exceptions import KustoError

from Kqlmagic.results import ResultSet
from Kqlmagic.parser import Parser
from Kqlmagic.parameterizer import Parameterizer

from Kqlmagic.log import Logger, logger, set_logger, create_log_context, set_logging_options
from Kqlmagic.display import Display
from Kqlmagic.database_html import Database_html
from Kqlmagic.help_html import Help_html
from Kqlmagic.kusto_engine import KustoEngine
from Kqlmagic.kql_engine import KqlEngineError
from Kqlmagic.palette import Palettes, Palette
from Kqlmagic.cache_engine import CacheEngine
from Kqlmagic.cache_client import CacheClient

_MAGIC_NAME = 'kql'


@magics_class
class Kqlmagic(Magics, Configurable):
    """Runs KQL statement on a repository as specified by a connect string.

    Provides the %%kql magic."""

    auto_limit = Int(0, config=True, allow_none=True, help="Automatically limit the size of the returned result sets. Abbreviation: al")
    prettytable_style = Enum(
        ["DEFAULT", "MSWORD_FRIENDLY", "PLAIN_COLUMNS", "RANDOM"],
        "DEFAULT",
        config=True,
        help="Set the table printing style to any of prettytable's defined styles. Abbreviation: ptst",
    )
    short_errors = Bool(True, config=True, help="Don't display the full traceback on KQL Programming Error. Abbreviation: se")
    display_limit = Int(
        None,
        config=True,
        allow_none=True,
        help="Automatically limit the number of rows displayed (full result set is still stored). Abbreviation: dl",
    )
    auto_dataframe = Bool(False, config=True, help="Return Pandas dataframe instead of regular result sets. Abbreviation: ad")
    columns_to_local_vars = Bool(False, config=True, help="Return data into local variables from column names. Abbreviation: c2lv")
    feedback = Bool(True, config=True, help="Show number of records returned, and assigned variables. Abbreviation: f")
    show_conn_info = Enum(
        ["list", "current", "None"],
        "current",
        config=True,
        allow_none=True,
        help="Show connection info, either current, the whole list, or None. Abbreviation: sci",
    )
    dsn_filename = Unicode(
        "odbc.ini",
        config=True,
        help="Path to DSN file. "
        "When the first argument is of the form [section], "
        "a kql connection string is formed from the "
        "matching section in the DSN file. Abbreviation: dl",
    )
    plot_package = Enum(["matplotlib", "plotly"], "plotly", config=True, help="Set the plot package. Abbreviation: pp")
    table_package = Enum(
        ["prettytable", "pandas", "plotly", "qgrid"], "prettytable", config=True, help="Set the table display package. Abbreviation: tp"
    )
    last_raw_result_var = Unicode(
        "_kql_raw_result_", config=True, help="Set the name of the variable that will contain last raw result. Abbreviation: var"
    )
    enable_suppress_result = Bool(True, config=True, help="Suppress result when magic ends with a semicolon ;. Abbreviation: esr")
    show_query_time = Bool(True, config=True, help="Print query execution elapsed time. Abbreviation: sqt")
    plotly_fs_includejs = Bool(
        False,
        config=True,
        help="Include plotly javascript code in popup window. If set to False (default), it download the script from https://cdn.plot.ly/plotly-latest.min.js. Abbreviation: pfi",
    )

    validate_connection_string = Bool(
        True, config=True, help="Validate connectionString with an implicit query, when query statement is missing. Abbreviation: vc"
    )
    auto_popup_schema = Bool(True, config=True, help="Popup schema when connecting to a new database. Abbreviation: aps")

    json_display = Enum(["raw", "native", "formatted"], "formatted", config=True, help="Set json/dict display format. Abbreviation: jd")
    palette_name = Unicode(Palettes.DEFAULT_NAME, config=True, help="Set pallete by name to be used for charts. Abbreviation: pn")
    palette_colors = Int(Palettes.DEFAULT_N_COLORS, config=True, help="Set pallete number of colors to be used for charts. Abbreviation: pc")
    palette_desaturation = Float(Palettes.DEFAULT_DESATURATION, config=True, help="Set pallete desaturation to be used for charts. Abbreviation: pd")

    temp_folder_name = Unicode("{0}_temp_files".format(Constants.MAGIC_CLASS_NAME), config=True, help="Set the folder name for temporary files")
    export_folder_name = Unicode("{0}_exported_files".format(Constants.MAGIC_CLASS_NAME), config=True, help="Set the folder name  for exported files")
    cache_folder_name = Unicode("{0}_cache_files".format(Constants.MAGIC_CLASS_NAME), config=True, help="Set the folder name for cache files")

    # valid values: jupyterlab or jupyternotebook
    notebook_app = Enum(["jupyterlab", "jupyternotebook"], "jupyternotebook", config=True, help="Set notebook application used.")

    add_kql_ref_to_help = Bool(True, config=True, help="On {} load auto add kql reference to Help menu.".format(Constants.MAGIC_CLASS_NAME))
    add_schema_to_help = Bool(True, config=True, help="On connection to database@cluster add  schema to Help menu.")
    cache = Bool(False, config=True, help="Cache query results.")
    use_cache = Bool(False, config=True, help="use cached query results, instead of executing the query.")
    params_dict = Unicode(None, config=True, allow_none=True, help="paremeters dictionary name, if None, python shell user namespace will be used.")
    @validate("palette_name")
    def _valid_value_palette_name(cls, proposal):
        try:
            Palette.validate_palette_name(proposal["value"])
        except AttributeError as e:
            message = "The 'palette_name' trait of a {0} instance {1}".format(Constants.MAGIC_CLASS_NAME, str(e))
            raise TraitError(message)
        return proposal["value"]

    @validate("palette_desaturation")
    def _valid_value_palette_desaturation(cls, proposal):
        try:
            Palette.validate_palette_desaturation(proposal["value"])
        except AttributeError as e:
            message = "The 'palette_desaturation' trait of a {0} instance {1}".format(Constants.MAGIC_CLASS_NAME, str(e))
            raise TraitError(message)
        return proposal["value"]

    @validate("palette_colors")
    def _valid_value_palette_color(cls, proposal):
        try:
            Palette.validate_palette_colors(proposal["value"])
        except AttributeError as e:
            message = "The 'palette_color' trait of a {0} instance {1}".format(Constants.MAGIC_CLASS_NAME, str(e))
            raise TraitError(message)
        return proposal["value"]

    # [KUSTO]
    # Driver          = Easysoft ODBC-SQL Server
    # Server          = my_machine\SQLEXPRESS
    # User            = my_domain\my_user
    # Password        = my_password
    # If the database you want to connect to is the default
    # for the SQL Server login, omit this attribute
    # Database        = Northwind

    # Object constructor
    def __init__(self, shell):
        # constants
        Configurable.__init__(self, config=shell.config)
        Magics.__init__(self, shell=shell)

        set_logger(Logger())

        get_ipython().magic("matplotlib inline")

        # Add ourself to the list of module configurable via %config
        self.shell.configurables.append(self)

        ip = get_ipython()
        load_mode = _get_kql_magic_load_mode()

        if load_mode != "silent":
            html_str = """<html>
            <head>
            <style>
            .kql-magic-banner {
                display: flex; 
                background-color: #d9edf7;
            }
            .kql-magic-banner > div {
                margin: 10px; 
                padding: 20px; 
                color: #3a87ad; 
                font-size: 13px;
            }
            </style>
            </head>
            <body>
                <div class='kql-magic-banner'>
                    <div><img src='data:image/png;base64,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'></div>
                    <div>
                        <p>Kusto is a log analytics cloud platform optimized for ad-hoc big data queries. Read more about it here: http://aka.ms/kdocs</p>
                        <p>   &bull; kql language reference: Click on 'Help' tab > and Select 'kql referece'<br>
                          &bull; """+Constants.MAGIC_CLASS_NAME+""" configuarion: Run in cell '%config """+Constants.MAGIC_CLASS_NAME+"""'<br>
                          &bull; """+Constants.MAGIC_CLASS_NAME+""" syntax: Run in cell '%kql?'<br>
                          &bull; """+Constants.MAGIC_CLASS_NAME+""" upgrate syntax: Run in cell '!pip install """+Constants.MAGIC_PIP_REFERENCE_NAME+""" --upgrade'<br>
                    </div>
                </div>
            </body>
            </html>"""
            Display.show_html(html_str)
            Display.showInfoMessage("""{0} package is updated frequently. Run '!pip install {1} --upgrade' to use the latest version.<br>{0} version: {2}, source: {3}""".format(Constants.MAGIC_PACKAGE_NAME, Constants.MAGIC_PIP_REFERENCE_NAME, VERSION, Constants.MAGIC_SOURCE_REPOSITORY_NAME))
            # <div><img src='https://az818438.vo.msecnd.net/icons/kusto.png'></div>

            try:
                pypi_version = get_pypi_latest_version(Constants.MAGIC_PACKAGE_NAME)
                if pypi_version and compare_version(pypi_version) > 0:
                    Display.showWarningMessage("""{0} version {1} was found in PyPI, consider to upgrade before you continue. Run '!pip install {0} --upgrade'""".format(Constants.MAGIC_PACKAGE_NAME, pypi_version))
            except:
                pass

        _override_default_configuration(ip, load_mode)

        root_path = get_ipython().starting_dir.replace("\\", "/")

        folder_name = ip.run_line_magic("config", "{0}.temp_folder_name".format(Constants.MAGIC_CLASS_NAME))
        showfiles_folder_Full_name = root_path + "/" + folder_name
        if not os.path.exists(showfiles_folder_Full_name):
            os.makedirs(showfiles_folder_Full_name)
        # ipython will removed folder at shutdown or by restart
        ip.tempdirs.append(showfiles_folder_Full_name)
        Display.showfiles_base_path = root_path
        Display.showfiles_folder_name = folder_name
        Display.notebooks_host = Help_html.notebooks_host = os.getenv("AZURE_NOTEBOOKS_HOST")

        app = ip.run_line_magic("config", "{0}.notebook_app".format(Constants.MAGIC_CLASS_NAME))
        # add help link
        add_kql_ref_to_help = ip.run_line_magic("config", "{0}.add_kql_ref_to_help".format(Constants.MAGIC_CLASS_NAME))
        if add_kql_ref_to_help:
            Help_html.add_menu_item("kql Reference", "http://aka.ms/kdocs", notebook_app=app)
        if app is None or app != "jupyterlab":
            display(Javascript("""IPython.notebook.kernel.execute("NOTEBOOK_URL = '" + window.location + "'");"""))
            time.sleep(5)
        _set_default_connections()

    @needs_local_scope
    @line_magic(Constants.MAGIC_NAME)
    @cell_magic(Constants.MAGIC_NAME)
    def execute(self, line, cell="", local_ns={}):
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
            # Note: establish connection to kusto and submit query.

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

        set_logger(Logger(None, create_log_context()))

        # save globals and locals so they can be referenced in bind vars
        user_ns = self.shell.user_ns.copy()
        user_ns.update(local_ns)

        logger().debug("To Parsed: \n\rline: {}\n\rcell:\n\r{}".format(line, cell))
        try:
            parsed = None
            parsed_queries = Parser.parse("%s\n%s" % (line, cell), self)
            logger().debug("Parsed: {}".format(parsed_queries))
            result = None
            for parsed in parsed_queries:
                result = self.execute_query(parsed, user_ns)
            return result
        except Exception as e:
            if parsed:
                if parsed["options"].get("short_errors", self.short_errors):
                    Display.showDangerMessage(str(e))
                    return None
            elif self.short_errors:
                Display.showDangerMessage(str(e))
                return None
            raise

    def _get_connection_info(self, **options):
        mode = options.get("show_conn_info", self.show_conn_info)
        if mode == "current":
            return Connection.get_connection_list_formatted()
        elif mode == "list":
            return [Connection.get_current_connection_formatted()]
        return []

    def _show_connection_info(self, **options):
        msg = self._get_connection_info(**options)
        if len(msg) > 0:
            Display.showInfoMessage(msg)

    def submit_get_notebook_url(self):
        if self.notebook_app != "jupyterlab":
            display(Javascript("""IPython.notebook.kernel.execute("NOTEBOOK_URL = '" + window.location + "'");"""))

    def execute_query(self, parsed, user_ns, result_set=None):
        if Help_html.showfiles_base_url is None:
            window_location = user_ns.get("NOTEBOOK_URL")
            if window_location is not None:
                Help_html.flush(window_location, notebook_app=self.notebook_app)
            else:
                self.submit_get_notebook_url()

        query = parsed["query"].strip()
        options = parsed["options"]
        suppress_results = options.get("suppress_results", False) and options.get("enable_suppress_result", self.enable_suppress_result)
        connection_string = parsed["connection"]

        special_info = False
        if options.get("version"):
            print("{0} version: {1}".format(Constants.MAGIC_PACKAGE_NAME, VERSION))
            special_info = True

        if options.get("palette"):
            palette = Palette(
                palette_name=options.get("palette_name", self.palette_name),
                n_colors=options.get("palette_colors", self.palette_colors),
                desaturation=options.get("palette_desaturation", self.palette_desaturation),
                to_reverse=options.get("palette_reverse", False),
            )
            html_str = palette._repr_html_()
            Display.show_html(html_str)
            special_info = True

        if options.get("popup_palettes"):
            n_colors = options.get("palette_colors", self.palette_colors)
            desaturation = options.get("palette_desaturation", self.palette_desaturation)
            palettes = Palettes(n_colors=n_colors, desaturation=desaturation)
            html_str = palettes._repr_html_()
            button_text = "popup {0} colors palettes".format(n_colors)
            file_name = "{0}_colors_palettes".format(n_colors)
            if desaturation is not None and desaturation != 1.0 and desaturation != 0:
                file_name += "_desaturation{0}".format(str(desaturation))
                button_text += " (desaturation {0})".format(str(desaturation))
            file_path = Display._html_to_file_path(html_str, file_name, **options)
            Display.show_window(file_name, file_path, button_text=button_text, onclick_visibility="visible")
            special_info = True

        if options.get("popup_help"):
            help_url = "http://aka.ms/kdocs"
            # 'https://docs.loganalytics.io/docs/Language-Reference/Tabular-operators'
            # 'http://aka.ms/kdocs'
            # 'https://kusdoc2.azurewebsites.net/docs/queryLanguage/query-essentials/readme.html'
            # import requests
            # f = requests.get(help_url)
            # html = f.text.replace('width=device-width','width=500')
            # Display.show(html, **{"popup_window" : True, 'name': 'KustoQueryLanguage'})
            button_text = "popup kql help "
            Display.show_window("KustoQueryLanguage", help_url, button_text, onclick_visibility="visible")
            special_info = True

        if special_info and not query and not connection_string:
            return None

        try:
            #
            # set connection
            #
            conn = Connection.get_connection(connection_string, **options)

        # parse error
        except KqlEngineError as e:
            if options.get("short_errors", self.short_errors):
                msg = Connection.tell_format(connection_string)
                Display.showDangerMessage(str(e))
                Display.showInfoMessage(msg)
                return None
            else:
                raise

        # parse error
        except ConnectionError as e:
            if options.get("short_errors", self.short_errors):
                Display.showDangerMessage(str(e))
                self._show_connection_info(show_conn_info="list")
                return None
            else:
                raise

        try:
            # validate connection
            if not conn.options.get("validate_connection_string_done") and options.get("validate_connection_string", self.validate_connection_string):
                retry_with_code = False
                try:
                    conn.validate(**options)
                    conn.set_validation_result(True)
                except Exception as e:
                    msg = str(e)
                    if msg.find("AADSTS50079") > 0 and msg.find("multi-factor authentication") > 0 and isinstance(conn, KustoEngine):
                        Display.showDangerMessage(str(e))
                        retry_with_code = True
                    else:
                        raise e

                if retry_with_code:
                    Display.showInfoMessage("replaced connection with code authentication")
                    database_name = conn.get_database()
                    cluster_name = conn.get_cluster()
                    uri_schema_name = conn._URI_SCHEMA_NAME
                    connection_string = "{0}://code().cluster('{1}').database('{2}')".format(uri_schema_name, cluster_name, database_name)
                    conn = Connection.get_connection(connection_string, **options)
                    conn.validate(**options)
                    conn.set_validation_result(True)

            conn.options["validate_connection_string_done"] = True

            schema_file_path = None
            if options.get("popup_schema") or (
                not conn.options.get("auto_popup_schema_done") and options.get("auto_popup_schema", self.auto_popup_schema)
            ):
                schema_file_path = Database_html.get_schema_file_path(conn, **options)
                Database_html.popup_schema(schema_file_path, conn)

            conn.options["auto_popup_schema_done"] = True
            if not conn.options.get("add_schema_to_help_done") and options.get("add_schema_to_help"):
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

            params_dict_name = options.get('params_dict')
            dictionary = user_ns.get(params_dict_name) if params_dict_name is not None and len(params_dict_name) > 0 else user_ns
            parametrized_query = Parameterizer(dictionary).expand(query) if result_set is None else result_set.parametrized_query
            raw_query_result = conn.execute(parametrized_query, user_ns, **options)

            end_time = time.time()

            #
            # model query results
            #
            if result_set is None:
                fork_table_id = 0
                saved_result = ResultSet(raw_query_result, parametrized_query, fork_table_id=0, fork_table_resultSets={}, metadata={}, options=options)
                saved_result.metadata["magic"] = self
                saved_result.metadata["parsed"] = parsed
                saved_result.metadata["connection"] = conn.get_conn_name()
            else:
                fork_table_id = result_set.fork_table_id
                saved_result = result_set.fork_result(0)
                saved_result.feedback_info = []
                saved_result._update(raw_query_result)

            result = saved_result

            if not connection_string and Connection.connections:
                saved_result.metadata["conn_info"] = self._get_connection_info(**options)
            else:
                saved_result.metadata["conn_info"] = []

            saved_result.metadata["start_time"] = start_time
            saved_result.metadata["end_time"] = end_time

            if options.get("feedback", self.feedback):
                minutes, seconds = divmod(end_time - start_time, 60)
                saved_result.feedback_info.append("Done ({:0>2}:{:06.3f}): {} records".format(int(minutes), seconds, saved_result.records_count))

            if options.get("columns_to_local_vars", self.columns_to_local_vars):
                # Instead of returning values, set variables directly in the
                # users namespace. Variable names given by column names

                if options.get("feedback", self.feedback):
                    saved_result.feedback_info.append("Returning raw data to local variables")

                self.shell.user_ns.update(saved_result.to_dict())
                result = None

            if options.get("auto_dataframe", self.auto_dataframe):
                if options.get("feedback", self.feedback):
                    saved_result.feedback_info.append("Returning data converted to pandas dataframe")
                result = saved_result.to_dataframe()

            if options.get("result_var") and result_set is None:
                result_var = options["result_var"]
                if options.get("feedback", self.feedback):
                    saved_result.feedback_info.append("Returning data to local variable {}".format(result_var))
                self.shell.user_ns.update({result_var: result if result is not None else saved_result})
                result = None

            if options.get('cache') and not options.get('use_cache') and not isinstance(conn, CacheEngine):
                file_path = CacheClient().save(raw_query_result, conn.get_database(), conn.get_cluster(), parametrized_query, **options)
                if options.get("feedback", self.feedback):
                    saved_result.feedback_info.append("query results cached")

            if options.get('save_as') is not None:
                file_path = CacheClient().save(raw_query_result, conn.get_database(), conn.get_cluster(), parametrized_query, 
                                               filepath=options.get('save_as'), **options)
                if options.get("feedback", self.feedback):
                    saved_result.feedback_info.append("query results saved as {0}".format(file_path))

            saved_result.suppress_result = False
            saved_result.display_info = False
            if result is not None:
                if suppress_results:
                    saved_result.suppress_result = True
                elif options.get("auto_dataframe", self.auto_dataframe):
                    Display.showSuccessMessage(saved_result.feedback_info)
                else:
                    saved_result.display_info = True

            if result_set is None:
                saved_result._create_fork_results()
            else:
                saved_result._update_fork_results()

            # Return results into the default ipython _ variable
            self.shell.user_ns.update({options.get("last_raw_result_var", self.last_raw_result_var): saved_result})

            if result == saved_result:
                result = saved_result.fork_result(fork_table_id)
            return result

        except Exception as e:
            if not connection_string and Connection.connections and not suppress_results:
                # display list of all connections
                self._show_connection_info(**options)

            if options.get("short_errors", self.short_errors):
                Display.showDangerMessage(e)
                return None
            else:
                raise e

def _override_default_configuration(ip, load_mode):
    """override default {0} configuration from environment variable {1}_CONFIGURATION.
       the settings should be separated by a semicolon delimiter.
       for example:
       {1}_CONFIGURATION = 'auto_limit = 1000; auto_dataframe = True' """.format(Constants.MAGIC_CLASS_NAME, Constants.MAGIC_CLASS_NAME.upper())

    kql_magic_configuration = os.getenv("{0}_CONFIGURATION".format(Constants.MAGIC_CLASS_NAME.upper()))
    if kql_magic_configuration:
        kql_magic_configuration = kql_magic_configuration.strip()
        if kql_magic_configuration.startswith("'") or kql_magic_configuration.startswith('"'):
            kql_magic_configuration = kql_magic_configuration[1:-1]

        pairs = kql_magic_configuration.split(";")
        for pair in pairs:
            ip.run_line_magic("config", "{0}.{1}".format(Constants.MAGIC_CLASS_NAME, pair.strip()))

    app = os.getenv("{0}_NOTEBOOK_APP".format(Constants.MAGIC_CLASS_NAME.upper()))
    if app is not None:
        app = app.lower().strip().strip("\"'").replace("-", "").replace("/", "")
        app = {"jupyterlab": "jupyterlab", "jupyternotebook": "jupyternotebook", "lab": "jupyterlab", "notebook": "jupyternotebook"}.get(app)
        if app is not None:
            ip.run_line_magic("config", '{0}.notebook_app = "{1}"'.format(Constants.MAGIC_CLASS_NAME, app.strip()))


def _get_kql_magic_load_mode():
    load_mode = os.getenv("{0}_LOAD_MODE".format(Constants.MAGIC_CLASS_NAME.upper()))
    if load_mode:
        load_mode = load_mode.strip().lower()
        if load_mode.startswith("'") or load_mode.startswith('"'):
            load_mode = load_mode[1:-1].strip()
    return load_mode


def _set_default_connections():
    connection_str = os.getenv("{0}_CONNECTION_STR".format(Constants.MAGIC_CLASS_NAME.upper()))
    if connection_str:
        connection_str = connection_str.strip()
        if connection_str.startswith("'") or connection_str.startswith('"'):
            connection_str = connection_str[1:-1]

        ip = get_ipython()
        result = ip.run_line_magic(Constants.MAGIC_NAME, connection_str)
        if result and _get_kql_magic_load_mode() != "silent":
            print(result)


"""
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
        %config {0}.auto_dataframe = True
        %kql var1 << T | where c > 100 // var1 will hold the dataframe

If I use {0}.auto_dataframe = True, How can I get programmaticaly the last dataframe results of the last submitted query?
Answer: Execute the to_dataframe method on the result. For example:
        _kql_raw_result_.to_dataframe()

If I use {0}.auto_dataframe = True, How can I get programmaticaly the last raw results of the last submitted query?
Answer: _kql_raw_result_ holds the raw results.

""".format(Constants.MAGIC_CLASS_NAME)

import os
import time
import logging
# to avoid "No handler found" warnings.
from kql.log  import KQLMAGIC_LOGGER_NAME
logging.getLogger(KQLMAGIC_LOGGER_NAME).addHandler(logging.NullHandler())

from IPython.core.magic import Magics, magics_class, cell_magic, line_magic, needs_local_scope
from IPython.display import display_javascript
from IPython.core.display import display
from IPython.core.magics.display import Javascript


from traitlets.config.configurable import Configurable
from traitlets import Bool, Int, Unicode, Enum

from kql.version import VERSION
from kql.connection import Connection
from azure.kusto.data import KustoError
from kql.ai_client import AppinsightsError
from kql.la_client import LoganalyticsError

from kql.results import ResultSet
from kql.parser import Parser

from kql.log  import Logger, logger, set_logger, create_log_context, set_logging_options
from kql.display  import Display
from kql.database_html  import Database_html
from kql.help_html import Help_html
from kql.kusto_engine import KustoEngine



@magics_class
class Kqlmagic(Magics, Configurable):
    """Runs KQL statement on Kusto, specified by a connect string.

    Provides the %%kql magic."""


    auto_limit = Int(0, config=True, allow_none=True, help="Automatically limit the size of the returned result sets. Abbreviation: al")
    prettytable_style = Enum(['DEFAULT', 'MSWORD_FRIENDLY', 'PLAIN_COLUMNS', 'RANDOM'], 'DEFAULT', config=True, help="Set the table printing style to any of prettytable's defined styles. Abbreviation: ptst")
    short_errors = Bool(True, config=True, help="Don't display the full traceback on KQL Programming Error. Abbreviation: se")
    display_limit = Int(None, config=True, allow_none=True, help="Automatically limit the number of rows displayed (full result set is still stored). Abbreviation: dl")
    auto_dataframe = Bool(False, config=True, help="Return Pandas dataframe instead of regular result sets. Abbreviation: ad")
    columns_to_local_vars = Bool(False, config=True, help="Return data into local variables from column names. Abbreviation: c2lv")
    feedback = Bool(True, config=True, help="Print number of records returned, and assigned variables. Abbreviation: f")
    show_conn_list = Bool(True, config=True, help="Print connection list, when connection not specified. Abbreviation: scl")
    dsn_filename = Unicode('odbc.ini', config=True, help="Path to DSN file. "
                           "When the first argument is of the form [section], "
                           "a kql connection string is formed from the "
                           "matching section in the DSN file. Abbreviation: dl")
    plot_package = Enum(['matplotlib', 'plotly'], 'matplotlib', config=True, help="Set the plot package. Abbreviation: pp")
    table_package = Enum(['prettytable', 'pandas', 'plotly', 'qgrid'], 'prettytable', config=True, help="Set the table display package. Abbreviation: tp")
    last_raw_result_var = Unicode('_kql_raw_result_', config=True, help="Set the name of the variable that will contain last raw result. Abbreviation: var")
    enable_suppress_result = Bool(True, config=True, help="Suppress result when magic ends with a semicolon ;. Abbreviation: esr")
    show_query_time = Bool(True, config=True, help="Print query execution elapsed time. Abbreviation: sqt")
    plotly_fs_includejs = Bool(False, config=True, help="Include plotly javascript code in popup window. If set to False (default), it download the script from https://cdn.plot.ly/plotly-latest.min.js. Abbreviation: pfi")

    validate_connection_string = Bool(True, config=True, help="Validate connectionString with an implicit query, when query statement is missing. Abbreviation: vc")
    auto_popup_schema = Bool(True, config=True, help="Popup schema when connecting to a new database. Abbreviation: aps")

    showfiles_folder_name = Unicode('temp_showfiles', config=True, help="Set the name of folder for temporary popup files")



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

        get_ipython().magic('matplotlib inline')

        # Add ourself to the list of module configurable via %config
        self.shell.configurables.append(self)

    @needs_local_scope
    @line_magic('kql')
    @cell_magic('kql')
    def execute(self, line, cell='', local_ns={}):
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
        parsed_queries = Parser.parse('%s\n%s' % (line, cell), self)
        logger().debug("Parsed: {}".format(parsed_queries))
        result = None
        for parsed in parsed_queries:
            result = self.execute_query(parsed, user_ns)
        return result


    def execute_query(self, parsed, user_ns, result_set = None):
        if Display.showfiles_base_url is None:
            # display(Javascript("""IPython.notebook.kernel.execute("NOTEBOOK_URL = '" + window.location + "'")"""))
            notebook_url = user_ns.get("NOTEBOOK_URL")
            if notebook_url is not None:
                if notebook_url.startswith("http://localhost") or notebook_url.startswith("https://localhost"):
                    parts = notebook_url.split('/')
                    parts.pop()
                    Display.showfiles_base_url = '/'.join(parts) 
                else:
                    azure_notebooks_host = os.getenv('AZURE_NOTEBOOKS_HOST')
                    if azure_notebooks_host:
                        start = notebook_url.find('//') + 2
                        suffix = '.' + azure_notebooks_host[start:]
                    else:
                        suffix = '.notebooks.azure.com'
                    end = notebook_url.find(suffix)
                    # azure notebook environment, assume template: https://library-user.libray.notebooks.azure.com
                    if (end > 0):
                        start = notebook_url.find('//') + 2
                        library, user = notebook_url[start:end].split('-')
                        azure_notebooks_host = azure_notebooks_host or 'https://notebooks.azure.com'
                        Display.showfiles_base_url = azure_notebooks_host + '/api/user/' +user+ '/library/' +library+ '/html'
                    # assume just a remote kernel, as local
                    else:
                        parts = notebook_url.split('/')
                        parts.pop()
                        Display.showfiles_base_url = '/'.join(parts) 
                        # assumes it is at root
            else:
                print('popup may not work !!!')
                Display.showfiles_base_url = ''
                # raise ConnectionError('missing NOTEBOOK_URL') 
            Display.showfiles_base_url += "/" + self.showfiles_folder_name + "/"

            # print('NOTEBOOK_URL = {0} '.format(notebook_url))

        query = parsed['kql'].strip()
        options = parsed['options']
        suppress_results = options.get('suppress_results', False) and options.get('enable_suppress_result', self.enable_suppress_result)
        connection_string = parsed['connection']

        if options.get('version'):
            print('Kqlmagic version: ' + VERSION)

        if options.get('popup_help'):
            help_url = 'http://aka.ms/kdocs'
            # 'https://docs.loganalytics.io/docs/Language-Reference/Tabular-operators'
            # 'http://aka.ms/kdocs'
            # 'https://kusdoc2.azurewebsites.net/docs/queryLanguage/query-essentials/readme.html'
            # import requests
            # f = requests.get(help_url)
            # html = f.text.replace('width=device-width','width=500')
            # Display.show(html, **{"popup_window" : True, 'name': 'KustoQueryLanguage'})
            button_text = 'popup kql help '
            Display.show_window('KustoQueryLanguage', help_url, button_text)

        try:
            #
            # set connection
            #
            conn = Connection.get_connection(connection_string)

        # parse error
        except KqlEngineError as e:
            if options.get('short_errors', self.short_errors):
                msg = Connection.tell_format(connect_str)
                Display.showDangerMessage(str(e))
                Display.showInfoMessage(msg)
                return None
            else:
                raise

        # parse error
        except ConnectionError as e:
            if options.get('short_errors', self.short_errors):
                Display.showDangerMessage(str(e))
                list = Connection.get_connection_list_formatted()
                if len(list) > 0:
                    Display.showInfoMessage(list)
                return None
            else:
                raise

        try:
            # validate connection
            retry_with_code = False
            if options.get('validate_connection_string', self.validate_connection_string) and not conn.options.get('validate_connection_string'):
                validation_query = 'range c from 1 to 10 step 1 | count'
                try:
                    raw_table = conn.execute(validation_query)
                    conn.set_validation_result(True)
                except Exception as e:
                    msg = str(e)
                    if msg.find('AADSTS50079') > 0 and msg.find('multi-factor authentication') > 0 and isinstance(conn, KustoEngine):
                        Display.showDangerMessage(str(e))
                        retry_with_code = True
                    else:
                        raise e

            if retry_with_code:
                Display.showInfoMessage('replaced connection with code authentication')
                database_name = conn.get_database()
                cluster_name = conn.get_cluster()
                connection_string = "kusto://code().cluster('" +cluster_name+ "').database('" +database_name+ "')"
                conn = Connection.get_connection(connection_string)
                raw_table = conn.execute(validation_query)
                conn.set_validation_result(True)

            conn.options['validate_connection_string'] = True

            if options.get('popup_schema') or (options.get('auto_popup_schema', self.auto_popup_schema) and not conn.options.get('auto_popup_schema')):
                Database_html.popup_schema(conn)
            conn.options['auto_popup_schema'] = True

            if not query:
                #
                # If NO  kql query, just return the current connection
                #
                if not connection_string and Connection.connections and options.get('show_conn_list', self.show_conn_list) and not suppress_results:
                    Display.showInfoMessage(Connection.get_connection_list_formatted())
                return None
            #
            # submit query
            #
            start_time = time.time()

            raw_table = conn.execute(query, user_ns)

            end_time = time.time()
            elapsed_timespan = end_time - start_time

            #
            # model query results
            #
            if result_set is None:
                saved_result = ResultSet(raw_table, query, options)
                saved_result.magic = self
                saved_result.parsed = parsed
                saved_result.connection = conn.get_name()
            else:
                saved_result = result_set
                saved_result._update(raw_table)

            if not connection_string and Connection.connections and options.get('show_conn_list', self.show_conn_list):
                saved_result.conn_info = Connection.get_connection_list_formatted()

            saved_result.start_time = start_time
            saved_result.end_time = end_time
            saved_result.elapsed_timespan = elapsed_timespan
            self.shell.user_ns.update({ options.get('last_raw_result_var', self.last_raw_result_var) : saved_result })

            result = saved_result
            if options.get('feedback', self.feedback):
                minutes, seconds = divmod(elapsed_timespan, 60)
                saved_result.info.append('Done ({:0>2}:{:06.3f}): {} records'.format(int(minutes), seconds, saved_result.records_count))

            logger().debug("Results: {} x {}".format(len(saved_result), len(saved_result.columns_name)))

            if options.get('columns_to_local_vars', self.columns_to_local_vars):
                #Instead of returning values, set variables directly in the
                #users namespace. Variable names given by column names

                if options.get('feedback', self.feedback):
                    saved_result.info.append('Returning raw data to local variables [{}]'.format(', '.join(saved_result.columns_name)))

                self.shell.user_ns.update(saved_result.to_dict())
                result = None

            if options.get('auto_dataframe', self.auto_dataframe):
                if options.get('feedback', self.feedback):
                    saved_result.info.append('Returning data converted to pandas dataframe')
                result = saved_result.to_dataframe()

            if options.get('result_var') and result_set is None:
                result_var = options['result_var']
                if options.get('feedback', self.feedback):
                    saved_result.info.append('Returning data to local variable {}'.format(result_var))
                self.shell.user_ns.update({result_var: result if result is not None else saved_result})
                result = None

            if result is None:
                return None

            if not suppress_results:
                if options.get('auto_dataframe', self.auto_dataframe):
                    Display.showSuccessMessage(saved_result.info)
                else:
                    saved_result.display_info = True
            else:
                saved_result.suppress_result = True

            # Return results into the default ipython _ variable
            return result

        except Exception as e:
            if not connection_string and Connection.connections and options.get('show_conn_list', self.show_conn_list) and not suppress_results:
                # display list of all connections
                Display.showInfoMessage(Connection.get_connection_list_formatted())

            if options.get('short_errors', self.short_errors):
                Display.showDangerMessage(e)
                return None
            else:
                raise e



def load_ipython_extension(ip):
    """Load the extension in Jupyter."""

    # this fails in both Firefox and Chrome for OS X.
    # I get the error: TypeError: IPython.CodeCell.config_defaults is undefined

    # js = "IPython.CodeCell.config_defaults.highlight_modes['magic_kql'] = {'reg':[/^%%kql/]};"
    # display_javascript(js, raw=True)
    kql_magic_load_mode = os.getenv('KQLMAGIC_LOAD_MODE')
    if kql_magic_load_mode:
        kql_magic_load_mode = kql_magic_load_mode.strip().lower()
        if kql_magic_load_mode.startswith("'") or kql_magic_load_mode.startswith('"'):
            kql_magic_load_mode = kql_magic_load_mode[1:-1].strip()
    if kql_magic_load_mode != 'silent':
        html_str = """<html>
        <head>
        <style>
        .kqlmagic-banner {
            display: flex; 
            background-color: #d9edf7;
        }
        .kqlmagic-banner > div {
            margin: 10px; 
            padding: 20px; 
            color: #3a87ad; 
            font-size: 13px;
        }
        </style>
        </head>
        <body>
            <div class='kqlmagic-banner'>
                <div><img src='data:image/png;base64,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'></div>
                <div>
                    <p>Kusto is a log analytics cloud platform optimized for ad-hoc big data queries. Read more about it here: http://aka.ms/kdocs</p>
                    <p>   &bull; kql language reference: Click on 'Help' tab > and Select 'kql referece'<br>
                      &bull; Kqlmagic configuarion: Run in cell '%config kqlmagic'<br>
                      &bull; Kqlmagic syntax: Run in cell '%kql?'<br>
                      &bull; Kqlmagic upgrate syntax: Run 'pip install git+git://github.com/mbnshtck/jupyter-kql-magic.git --upgrade'<br>
                </div>
            </div>
        </body>
        </html>"""
        Display.show(html_str)
        Display.showInfoMessage("""Kqlmagic version: """ +VERSION+ """, source: https://github.com/mbnshtck/jupyter-kql-magic""")
        #<div><img src='https://az818438.vo.msecnd.net/icons/kusto.png'></div>
    result = ip.register_magics(Kqlmagic)
    _override_default_configuration(ip, kql_magic_load_mode)
    _set_default_connections(ip, kql_magic_load_mode)

    # add help link
    Help_html.add_menu_item('kql Reference', 'http://aka.ms/kdocs')

    root_path = get_ipython().starting_dir

    folder_name = ip.run_line_magic('config', 'Kqlmagic.showfiles_folder_name')
    # print('folder_name = ' + folder_name)
    showfiles_folder_Full_name = root_path + '/' + folder_name
    if not os.path.exists(showfiles_folder_Full_name):
        os.makedirs(showfiles_folder_Full_name)
    ip.tempdirs.append(showfiles_folder_Full_name)
    Display.showfiles_base_path = showfiles_folder_Full_name + '/'
    # print(Display.showfiles_base_path)

    # get notebook location
    display(Javascript("""IPython.notebook.kernel.execute("NOTEBOOK_URL = '" + window.location + "'");"""))
    folder_name = ip.run_line_magic('config', 'Kqlmagic.showfiles_folder_name')
    time.sleep(5)
    return result

def unload_ipython_extension(ip):
    """Unoad the extension in Jupyter."""
    del ip.magics_manager.magics['cell']['kql']
    del ip.magics_manager.magics['line']['kql']

def _override_default_configuration(ip, load_mode):
    """override default Kqlmagic configuration from environment variable KQL_MAGIC_CONFIGURATION.
       the settings should be separated by a semicolon delimiter.
       for example:
       KQL_MAGIC_CONFIGURATION = 'auto_limit = 1000; auto_dataframe = True' """

    kql_magic_configuration = os.getenv('KQL_MAGIC_CONFIGURATION')
    if kql_magic_configuration:
        kql_magic_configuration = kql_magic_configuration.strip()
        if kql_magic_configuration.startswith("'") or kql_magic_configuration.startswith('"'):
            kql_magic_configuration = kql_magic_configuration[1:-1]

        pairs = kql_magic_configuration.split(';')
        for pair in pairs:
            ip.run_line_magic('config',  'Kqlmagic.{0}'.format(pair.strip()))

def _set_default_connections(ip, load_mode):
    kql_magic_connection_str = os.getenv('KQL_MAGIC_CONNECTION_STR')
    if kql_magic_connection_str:
        kql_magic_connection_str = kql_magic_connection_str.strip()
        if kql_magic_connection_str.startswith("'") or kql_magic_connection_str.startswith('"'):
            kql_magic_connection_str = kql_magic_connection_str[1:-1]

        result = ip.run_line_magic('kql',  kql_magic_connection_str)
        if (load_mode != 'silent'):
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
        %config Kqlmagic.auto_dataframe = True
        %kql var1 << T | where c > 100 // var1 will hold the dataframe

If I use Kqlmagic.auto_dataframe = True, How can I get programmaticaly the last dataframe results of the last submitted query?
Answer: Execute the to_dataframe method on the result. For example:
        _kql_raw_result_.to_dataframe()

If I use Kqlmagic.auto_dataframe = True, How can I get programmaticaly the last raw results of the last submitted query?
Answer: _kql_raw_result_ holds the raw results.

"""

# HISTORY

## Version 0.1.114

  - ### New Major Feature: kqlmagic kernel over python kernel
    - to switch from python kernel to kqlmagic kernel execute ```%kql --activate_kernel```
    - to switch back from kqlmagic kernel to python kernel execute ```%kql --deactivate_kernel```
    - Once kqlmagic kernel activated:
      - By default code cells are executed as kqlmagic cells, unless the first word starts with a cell magic prefix "%%"
      - If the first word in the cell starts with the cell magic "%%" prefix, it will be executed by the proper cell magic (all python kernel cell magics are supported)
      - If the first word in the cell starts with the line magic "%" prefix, it will be executed by the proper line magic in the context of kqlmagic cell (all python kernel line magics are supported)
      - Additional %%py, %%pyro and %%pyrw cell magics (only in kqlmagic kernel) are supported to execute python code (see more info on py, pyro and pyrw kqlmagic commands below)
    - All python kernel extensions and magics are supported
    - To execute python cells specify %%py or %%pyro or %%py as the first word in the cell
    - All python kernel extensions
    - All python kernel cell magics are supported, including kqlmagic "%%kql"
    - All python kernel line magics are supported, including kqlmagic "%kql"

  - ### New platform: Support Kqlmagic on Azure Machine Learning service 'azureml'

    - **Note: KQLMAGIC_AZUREML_COMPUTE environment variable should be set, otherwise popup windows might not function properly.**
    
  - ### New SSO Authentication Feature: -try_vscode_login option

    - Before authenticating with the specified connection string, will try first to get token from Visual Studio Code Azure Account login.
    - It will use the connection string tenant id (if specified) as a parameter to AzAzure Account
    - if fail to get token from vscode, use the connection string to authenticate
    - for example:
        - ```%kql azureDataExplorer://code;cluster='help';database='Samples' -try_vscode_login```
  
  - ### New SSO Authentication Feature: -try_msi option

    - Before authenticating with the specified connection string, will try first to get token from MSI local endpoint.
    - if fail to get token from endpoint, use the connection string to authenticate
    - Expects as parameter a dictionary with the optional MSI params: **resource, client_id/object_id/mis_res_id, cloud_environment, timeout**

        - **timeout**: If provided, must be in seconds and indicates the maximum time we'll try to get a token before raising MSIAuthenticationTimeout
        - **client_id**: Identifies, by Azure AD client id, a specific explicit identity to use when authenticating to Azure AD. Mutually exclusive with object_id and msi_res_id.
        - **object_id**: Identifies, by Azure AD object id, a specific explicit identity to use when authenticating to Azure AD. Mutually exclusive with client_id and msi_res_id.
        - **msi_res_id**: Identifies, by ARM resource id, a specific explicit identity to use when authenticating to Azure AD. Mutually exclusive with client_id and object_id.
        - **cloud_environment** (msrestazure.azure_cloud.Cloud): A targeted cloud environment
        - **resource** (str): Alternative authentication resource, default is https://management.core.windows.net/'.
  
    - for example:
        - ```%kql azureDataExplorer://code;cluster='help';database='Samples' -try_msi={"client_id":"00000000-0000-0000-0000-000000000000"}```

  - ### New Authentication Feature: -try_token option (bring your own token)
    - Before authenticating with the specified connection string, will try first to authenticate with this token.
    - Expects as a parameter a dictionary with AAD v1 or v2 token properties. At least: tokenType/token_type, accessToken/access_token
    - for example:
        - ```%kql azureDataExplorer://code;cluster='help';database='Samples' -try_token={"tokenType":"bearer","accessToken":"<your-token-string>"}```

  - ### New Authentication Feature: MSAL interactive login
    - to use this mode connection string should be set to "code" mode.
    - to force using this mode, 'code_auth_interactive_mode' option must be set to "auth_code"
    - Otherwise 'device_code' will be used.
    - If 'code_auth_interactive_mode' option is set to "auto", then if kernel is located on the local machine it will use "auth_code" otherwise "device_code".

  - ### New Feature: Control dependencies
  
  - Features that require a missing dependency will be disabled.
  - Custom install is enabled by installing KqlmagicCustom instaed of Kqlmagic.
    - Many custumization combinations are available. (see custom install in README.md)
    - KqlmagicCustom provides full control over which dependencies will be installed, by specifying extra requires
      - for example ``` pip install KqlmagicCustom[extended, pandas, plotly] ```
  - install of Kqlmagic is the same as install of KqlmagicCustom[default]
  - if KqlmagicCustom was installed KQLMAGIC_EXTRAS_REQUIRE environment variable should be set to the same list of extras that were used to install (if not set, 'default' will be used).
    - for example: ```KQLMAGIC_EXTRAS_REQUIRE=<extra1>,<extra2>```
  - on Kqlmagic load, will raise an error for missing MANDATORY packages
  - on Kqlmagic load will warn on missing packages that were specified in KQLMAGIC_EXTRAS_REQUIRE (if not set, 'default' will be used)
  - dependencies warning can also be disabled by setting 'warn_missing_dependencies' option to False

  - ### New result properties 

    - **cursor** - displays the Cursor property that returned with the query results (see KQL Database cursors in KQL documentation). This value is required for queries that use the Cursor functions: current_cursor(), cursor_after() and cursor_before_or_at()


  - ### New options:
  
    - **warn_missing_dependencies** - bool - On Kqlmgaic load, warn missing dependencies (default: True)
    - **warn_missing_env_variables** - bool - On Kqlmagic load, warn missing environment variables (default: True)
    - **assign_var** - str - If specified, the result will be assigned to this variable in user's namespace
    - **cursor_var** - str - If specified, the cursor value returned from the query will be assigned to this variable in user's namespace
    - **request_cache_max_age** - int - specifies, in seconds, the maximum amount of time a http cached response is valid for. If set to 0, will bypass the response cache and always query the downstream services. If set to None, will use cached reponse as long it doesn't expires.
    - **allow_single_line_cell** - bool - When set to True, allows %%kql cell magic to include one line only, without body. (default: True)
    - **allow_py_comments_before_cell** - bool - When set to True, allows %%kql cell magic to be prefixed by python comments. (default: True)
    - **kqlmagic_kernel** - bool - When set to True, Kqlmagic kernel will be active. (can be set on load, and by --active_kernel commad)
    - **debug** - bool - Used internally for debug only, when set to True, debug prints are displayed  (can be set on load only)
    - **cache** - str - Cache query results to be saved to the specified folder. (can be set on load, and by --cache commad)
    - **use_cache** - str - Use cached query results from the specified folder, instead of executing the query. (can be set on load, and by --use_cache commad)
    - **extras_require** - str - A read-only option, can be set on Kqlmagic load, comma separated list of setup extras_require values that should be the same as specified at KqlmagicCustom install
    - **is_magic** - bool - A read-only option, set internally, that specify whether it is runing as magic or an imported module.
    - **kernel_id** - str - A read-only option, set internally, specify current notebook kernel_id as set by ipython kernel
    - **code_auth_interactive_mode** - str - Sets code authentication interative mode, if "auto" is set, "auth_code" will be seleted if kernel is local, otherwise "device_code" is used. valid values: "auto", "device_code" and "auth_code". (default: "device_code")


  - ### Support additional Azure Data Explorer Client Request Properties

    - **materialized_view_shuffle**: A hint to use shuffle strategy for materialized views that are referenced in the query. The property is an array of materialized views names and the shuffle keys to use. examples: 'dynamic([ { "Name": "V1", "Keys" : [ "K1", "K2" ] } ])' (shuffle view V1 by K1, K2) or 'dynamic([ { "Name": "V1" } ])' (shuffle view V1 by all keys) [dict]
    - **query_cursor_disabled**: Disables usage of cursor functions in the context of the query. [bool]
    - **query_force_row_level_security**: If specified, forces Row Level Security rules, even if row_level_security policy is disabled [bool]
    - **query_python_debug**: If set, generate python debug query for the enumerated python node (default first). [int]
    - **query_results_apply_getschema** If set, retrieves the schema of each tabular data in the results of the query instead of the data itself. [bool]
    - **query_results_cache_max_age**: If positive, controls the maximum age of the cached query results which Kusto is allowed to return [str]
    - **request_block_row_level_security**: If specified, blocks access to tables for which row_level_security policy is enabled [bool]
    - **request_description**: Arbitrary text that the author of the request wants to include as the request description. [str]
    - **request_external_table_disabled**: If specified, indicates that the request cannot invoke code in the ExternalTable. [bool]
    - **request_impersonation_disabled**: If specified, indicates that the service shouldn't impersonate the caller's identity. [bool]
    - **request_remote_entities_disabled**: If specified, indicates that the request cannot access remote databases and clusters. [bool]
    - **request_sandboxed_execution_disabled**: If specified, indicates that the request cannot invoke code in the sandbox. [bool]
  
  - ### New envrironment variables:

    - **KQLMAGIC_KERNEL**
      - Specify whether Kqlmagic kernel should be activated on Kqlmagic load. Valid values are True or False.<br>
      - Same as executing --activate_kernel / --deactivate_kernel


    - **KQLMAGIC_EXTRAS_REQUIRE (or KQLMAGIC_EXTRAS_REQUIRES)**
  
    - if KqlmagicCustom was installed KQLMAGIC_EXTRAS_REQUIRE environment variable should be set to the same list of extras that were used to install (if not set, 'default' will be used)
    - Default: 'default' (all extras)
    - for example:
        - ```KQLMAGIC_EXTRAS_REQUIRE=plotly,pandas,sso```
        - ```KQLMAGIC_EXTRAS_REQUIRE=default```
        - ```KQLMAGIC_EXTRAS_REQUIRE=jupyter-basic,widgets```
    - on Kqlmagic load will warn on missing packages that were specified in KQLMAGIC_EXTRAS_REQUIRE (if not set, 'default' will be used)
    - Features that require a missing dependency will be disabled.

    - **KQLMAGIC_AZUREML_COMPUTE**
  
      - Specified azureml backend compute host address. If not specified popup windows might not work properly.
      - Default: value taken from KQLMAGIC_NOTEBOOK_SERVICE_ADDRESS.
      - for example:
          - ```KQLMAGIC_AZUREML_COMPUTE=https://kqlmagic.eastus.instances.azureml.ms```

    - **KQLMAGIC_DEBUG**
      - Used internally for debug only
      - If exist on Kqlmagic load and set to True, debug prints are displayed

    - **KQLMAGIC_CACHE**
      - Specifies a folder name for storing query results that can be used as cache for same queries.
      - valid value if sepcified is a valid folder string
      - Same as executing the --cache command
      - The cache will be used by queries that will match and --use_cache is set to same folder (see: --use_cache command, KQLMAGIC_USE_CACHE environment variable, and cache://<myCacheFolder> schema)

    - **KQLMAGIC_USE_CACHE**
      - Specifies a folder that holds cached query results.
      - valid value if sepcified is a valid folder string
      - Same as executing the --use_cache command
      - for query that match an entry in specified folder cache, result will be taken from cache folder (see: --cache command and KQLMAGIC_CACHE environment variable, and cache://<myCacheFolder> schema)


  - ### New Command: --bug-report command
  
    - should be invoked immediately after something goes wrong.
    - gathers last cell execution state, ready to be copy/paste to Kqlmagic github issues (you must have an account in github)
      - Kqlmagic version
      - platform information
      - python information
      - Kqlmagic dependencies version 
      - Kqlmagic default options
      - Kqlmagic connections
      - Kqlmagic environment variables
      - Last kql execution context (options, parameters, tags, request, response), including error stack
    - for example:
      - ```%kql --bug-report```

  - ### New Command: --conn command

    - Displays connections info
    - for example:
      - ```%kql --conn // list connections```
      - ```%kql --conn "Samples@help" // display connection's details```

  - ### New Command: --cache command

    - Enables / Disables caching query result to the spacified folder
    - If a folder is not specified caching is disabled
    - The cache will be used by queries that will match and --use_cache is set to same folder
    - OPTIONAL: Stored cached data can be also queried using the cache schema: cache://<cache-folder-name>
    - Useful for demos and for scenarios when queries results must be preserved 
    - for example:
      - ```%kql --cache "myCachefolder"    // enables cache in myCachefolder folder```
      - ```%kql --cache                    // disables caching```

  - ### New Command: --use_cache command

    - Enables / Disables usage of cached query results data stored in the spacified folder
    - If a folder is not specified usage of cached data is disabled
    - for query that match an entry in specified folder cache, result will be taken from that cache folder
    - OPTIONAL: Stored cached data can be also queried using the cache schema: cache://<cache-folder-name>
    - Useful for demos and for scenarios when queries results must be preserved 
    - for example:
      - ```%kql --use_cache "myCachefolder"   // enables usage of cached data in myCachefolder folder```
      - ```%kql --use_cache                   // disables usage of cached data```


  - ### New Feature: sequence of cells within one %%kql cell

    - %%kql cell can include multiple blocks of lines separated by one or more empty line. And each block may be either:
      - multi line kql query
        - must be followed by an empty line or end of cell
        - optional, block can start with %%kql
      - single line kql query
        - optional, line can start with %kql
      - kqlmagic command
        - -optional line can start with %kql
      - ipython line magic
      - ipython cell magic
        - must be followed by an empty line or end of cell
    - A cell magic block contains all lines till new cell magic starts or end of cell
    - A sequence of line magics can be in one block
    - for example:
    - ```
      %%kql
      StormEvents
      | where Timestamp > ago(31d)
      | summarize TotalEvents = count()

      %%pyro
      from IPython.display import HTML
      def counter_to_htm_msg(title:str, value):
          return HTML(f"<p><FONT SIZE=3>{title}</FONT></p><p><FONT SIZE=7><b>{int(str(value)):,d}</b></FONT></p>")

      counter_to_htm_msg("Total Storm Events", _kql_raw_result_[0]["Count"])
      ```

  - ### New Commands: --py/pyro/pyrw command

    - Execute python code from within a %%kql cell
    - pyro (python readonly) will execute but won't change user namespace
    - pyrw (python read and write) will execute and it can change user namespace
    - py is the same as pyrw
    - py is different from the ipython builtin %%python cell magic as it runs in the user's name space, and if last statement is an expression it returns its value
    - for example:
    - ```%kql --py print("hello world")```
    - 
      ```
      %%kql
      StormEvents
      | where Timestamp > ago(31d)
      | summarize TotalEvents = count()

      --py
      from IPython.display import HTML
      def show_counter(title:str, value):
          return HTML(f"<p><FONT SIZE=3>{title}</FONT></p><p><FONT SIZE=7><b>{int(str(value)):,d}</b></FONT></p>")
      show_counter("Total number of storm events in last months", _kql_raw_result_[0]["TotalEvents"])
      ```
    - magic syntax for py/pyro/pyrw from within %%kql magic can also is enable for example:
    - ```
      %%kql
      StormEvents
      | where Timestamp > ago(31d)
      | summarize TotalEvents = count()

      %%py
      from IPython.display import HTML
      def show_counter(title:str, value):
          return HTML(f"<p><FONT SIZE=3>{title}</FONT></p><p><FONT SIZE=7><b>{int(str(value)):,d}</b></FONT></p>")
      show_counter("Total number of storm events in last months", _kql_raw_result_[0]["TotalEvents"])
      ```
  - ### New Command: --line_magic command

    - Execute an ipython line magic within a %%kql cell
    - for example:
    - ```
      %%kql 
      --line_magic %env VAR=VALUE
      ```
    - line magic prefix '%' is optional
    - for example:
    - ```
      %%kql
      --line_magic env VAR=VALUE
      ```
    - --line_magic command name is optional, if line magic is written with the line magic prefix '%'
    - for example:
    - ```
      %%kql
      %env VAR=VALUE
      ```

  - ### New Command: --activate_kernel command

    - Activates kqlmagic kernel over python kernel (see more above about this new feature)

  - ### New Command: --deactivate_kernel command

    - Deactivates kqlmagic kernel over python kernel. Switch back to  python kernel
  
  - ### New Command: --cell_magic command

    - Execute an ipython cell magic within a %%kql cell
    - for example:
    - ```
      %%kql
      --cell_magic %%script bash
        for i in 1 2 3; do
            echo $i
        done
      ```
    - cell magic prefix '%%' is optional
    - for example:
    - ```
      %%kql
      --cell_magic script bash
        for i in 1 2 3; do
            echo $i
        done
      ```
    - --cell_magic command name is optional, if cell magic is written with the cell magic prefix '%%'
    - for example:
    - ```
      %%kql
      %%script bash
        for i in 1 2 3; do
            echo $i
        done
      ```


  - ### New Feature: support ADX SQL query syntax
  
    - support EXPLAIN - translate SQL query to KQL query
    - SQL to Kusto cheat sheet: https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/sqlcheatsheet
    - MS-TDS/T-SQL Differences between Kusto Microsoft SQL Server: https://docs.microsoft.com/en-us/azure/data-explorer/kusto/api/tds/sqlknownissues
    - for example:
    - ``` sql
      %%kql 
      EXPLAIN // translate SQL queries to KQL
      SELECT COUNT_BIG(*) as C FROM StormEvents 
      ```
    - ``` sql
      %%kql 
      SELECT * FROM dependencies
      LEFT OUTER JOIN exception
      ON dependencies.operation_Id = exceptions.operation_Id
      ```

  - ### New help topics
  
    - 'sql' - Azure Data Explorer SQL support
    - 'aria - How to query Aria's Azure Data Explorer databases.
    - 'env' - Lists all Kqlmagic environment variables and their purpose.
    - 'options' - Lists the available options, and their impact on the submit query command.
    - for example:
      - ```%kql --help "sql"```
      - ```%kql --help "aria"```
      - ```%kql --help "saw"```
      - ```%kql --help "env"```
      - ```%kql --help "options"```

  - ### New feature - kql_stop Kqlmagic Module
    - kql_stop() was added to module, that resets Kqlmagic (equivalent to %unload_ext when used as a magic)

  - ### Improvements

    - Auto detect of Azure Machine Learning 'azureml' jupyter environment
    - Auto detect of Visual Studio Code on Linux
    - support supress output, using ";" as last character (whitespace characters are ignored) for all commands
      - to suppress a cell input, last line should include only the semicolon ";" (whitespace characters are ignored)
    - options can be before or after commands parameter
    - %%kql cell query can contain a mix of queries and commands in the same cell separated by empty line
    - allow single line %%kql cell. To disable it, set allow_single_line_cell option to False
    - allow %%kql cell to be prefixed by python comments. To disable it, set allow_py_comments_before_cell option to False

  - ### Fix

    - fix connection list display
    - fix sso cleanup on Kqlmagic load
    - fix parse short kusto cluster name in connection string
    - fix platform specific modules to be imported on right platform only
    - fix performance hit on null logger
    - fix token claims decode
    - fix parse option for Kqlmagic commands
    - fix popup windows for Kqlmagic commands
    - fix file system memory leaks
    - fix http requests memory leaks
    - fix suppress output for cell Kqlmagic, won't suppress if ";" is prefixed by not whistespace chars
    - fix suppress output for commands
    - fix commands' string parameter can have whitespaces
    - fix --config to return value when a key is set
    - fix nested activation of kqlmagic
    - fix default request cache control to be no cache
    - fix load/unload/reload reset Kqlmagic state
    - fix common Kqlmagic magic state with imported kql from Kqlmagic package
    - fix conversion of dynamic value 0
    - fix timedelta_to_timespan
    - fix Kqlmagic init, setting help menu in JupyterLab
    - fix 'missing keys' error when user/password auth is initiated after code auth


  - ### Other

    - replace AAD adal package with msal package
    - remove package six
    - remove package pytz
    - remove package jwt
    - remove package seaborn
    - remove old Visual Studio Enterprise project files
    - fix Pylance warnings / errors
    - add SECURITY.md file
    - update README.md and README.rst


## Version 0.1.113


  - ### Fix
  
    - fix Kqlmagic failure (pandas exception) when auto_dataframe option is set to True


## Version 0.1.112


  - ### New Feature -enable_curly_brackets_params option

    - when set, strings within curly brackets will be evaluated as a python expression, and if evaluation succeeds result will replace the string (including the curly brackets).
    - if evaluation fails it won't be modified, including the curly brackets.
    - to escape curly brackets they must be doubled {{something}}.
    - Abbreviation: 'ecbp'
    - type mapping from python to KQL types is as follows:
        - int -> long
        - float -> real
        - str -> string
        - bool -> bool
        - datetime -> datetime
        - timedelta -> timespan
        - dict, list, set, tuple -> dynamic (only if can be serialized to json)
        - pandas dataframe -> view table
        - None -> null
        - unknown, str(value) == 'nan' -> real(null)
        - unknown, str(value) == 'NaT' -> datetime(null)
        - unknown str(value) == 'nat' -> time(null)
        - other -> string
    - for example:
        - count = 10
        - interval = timedelta(days=5, hours=3)
        - %kql -ecbp requests | where timestamp > ago({interval}) | limit {count+5}

        - p_dict = {'p_limit':20, 'p_not_state':'IOWA'}
        - %%kql
            - -ecbp -params_dict=p_dict
            - StormEvents
            - | where State != {p_not_state}
            - | summarize count() by State
            - | sort by count_
            - | limit {p_limit}


  - ### New syntax Feature: enable KQL style // comments in commands and options

    - comments can be used within all lines of Kqlmagic, not only in query part
    - for example:
        - %kql azureDataExplorer://code;cluster='help';database='Samples' // -try_azcli_login
        - %%kql
            - //-show_query
            - requests 
            - | where timestamp > ago({interval}) 
            - | limit {count+5}
  
        - %kql // --version

  - ### New Log Analytics / Application Insights Feature: +timespan query property

    - enables to set the 'timespan' property in requests to Log Analytics or Application Insights.
    - The timespan over which to query data. This is an ISO8601 time period value. This timespan is applied in addition to any that are specified in the query expression.
    - see: https://docs.microsoft.com/en-us/rest/api/application-insights/query/get
    - for example:
        - ```%kql +timespan="P1Y2M10DT2H30M/2008-05-11T15:30:00Z" ...your-query...```
        - ```%kql +timespan="2007-03-01T13:00:00Z/P1Y2M10DT2H30M" ...your-query...```
        - ```%kql +timespan="2007-03-01T13:00:00Z/2008-05-11T15:30:00Z" ...your-query...```
        - ````%kql +timespan=timedelta(days=5,minutes=22,seconds=2) ...your-query...```
        - ```%kql +timespan=[timedelta(days=5,minutes=22,seconds=2),datetime.now()] ...your-query...```
        - ```%kql +timespan=["P1Y2M10DT2H30M",datetime.now()] ...your-query...```
        - ```%kql +timespan=[ten_years_ago,"P1Y2M10DT2H30M"] ...your-query...```


  - ### New Log Analytics Feature: +workspaces query property

    - enables to set the 'workspaces' property in requests to Log Analytics.
    - For either implicit or explicit cross-application queries, specify resources you will be accessing
    - see https://dev.loganalytics.io/documentation/Using-the-API/Cross-Resource-Queries
    - for example:
        - ```%kql +workspaces=["AIFabrikamDemo1", "AIFabrikamDemo2"] ...your-query...```
        - ```%kql +workspaces='3cc2e581-5032-4ccf-ac05-844b2867ee15' ...your-query...```


  - ### New Application Insights Feature: +applications query property

    - enables to set the 'applications' property in requests to Application Insights.
    - For either implicit or explicit cross-application queries, specify resources you will be accessing
    - see: https://dev.applicationinsights.io/documentation/Using-the-API/Cross-Resource-Queries
    - for example:
        - ```%kql +applications=["AIFabrikamDemo"] ...your-query...```
        - ```%kql +applications='3cc2e581-5032-4ccf-ac05-844b2867ee15' ...your-query...```


  - ### New SSO Authentication Feature: -try_azcli_login option

    - Before authenticating with the specified connection string, will try first to get token from Azure CLI.
    - It will use the connection string tenant id (if specified) as a parameter to Azcli
    - if socket not found use the connection string to authenticate
    - for example:
        - ```%kql azureDataExplorer://code;cluster='help';database='Samples' -try_azcli_login```

  - ### New SSO Authentication Feature: -try_azcli_login_subscription option

    - Before authenticating with the specified connection string, will try first to get token from Azure CLI using the subscription as a parameter to get the right token.
        - if socket not found use the connection string to authenticate
        - for example:
            - ```%kql azureDataExplorer://code;cluster='help';database='Samples' -try_azcli_login_subscription='49998620-4d47-4ab8-88d1-d92ea58902e9'```

  - ### New Feature -plotly_layout option

    - enables to specify plotly layout properties, when set they override the defualt layout properties (see plotly documentation on the layout properties)
    - Abbreviation: 'pl'
    - for example:
        - ```%kql -pl={"width":900"} ...your-query... ```

  - ### New Feature -dynamic_to_dataframe option
  
    - controls to what dataframe type should an kql dynamic value be translated. 
    - Either 'object' or 'str'
    - for example:
        - ```%kql --config 'dynamic_to_dataframe=str'```
        - ```%kql -dynamic_to_dataframe='str' ...your-query...```

  - ### New Feature -temp_folder_location option
  
    - Set the location of the temp_folder, either within starting working directory or user workspace directory.
    - value values are: ["auto", "starting_dir", "user_dir"]
        - starting_dir - is the location from where jupyter was launched
        - user_dir - is  user's home location
    - if set to 'auto' (default), location will be based on the notebook application
    - for example:
        - ```%env KQLMAGIC_CONFIGURATION='temp_folder_location=starting_dir'```

  - ### Enhancements

    - support ymin and ymax KQL render operator attributes
    - added to table_package: "auto" and "pandas_html_table_schema"
        - when set to 'auto' (default) table package will be based on the notebook application
        - 'pandas_html_table_schema' is an interactive object supported by only some by some to the applications (i.e nteract, azure data studio) (see pandas documentation)
    - added title to all popup windows

  - ### Improvements
  
    - Auto detection of Azure Data Studio, nteract and Azure Notebooks
    - Auto detect local/remote kernel location
    - local files server for Azure Data Studion, Visual Studio Code and nteract
    - logs
    - error messages

  - ### Fix

    - open 'what's new' popup window in Azure Data Studio, Visual Studio Code and nteract
    - deep linking to Kusto.Explorer and Kusto.Webexplorer in Azure Data Studio, Visual Studio Code and nteract
    - popwindow for Visual Studio Code
    - create temporary file in folder that user have permission to access
    - close zombie popup windows
    - option parsing of type dict
    - option parsing accept None value
    - display of dynamic column by pandas when using pandas_show_schema mode
    - surpress pandas warnings
    - 'out of bounds nanoseconds timestamp' converting datetime to pandas dataframe
    - 'can't subtract offset-naive and offset-aware datetimes' converting datetime to pandas dataframe
    - decimal type convertion to python
    - set query properties using the '+' prefix
    - json serialization of bytes, decimal, datetime and timedelta


## Version 0.1.111

  - ### New Jupyter front end supported

    - Kqlmagic can be now used within desktop **"nteract"**

  - ### New Feature --banner command

    - **--banner** command
      - displays the Kqlmagic banner. Useful when Kqlmagic is loaded in silent mode, or is preloaded or when Kqlmagic is used just as a module.
      - ```%kql -did ...``` # display the table or rendered chart linked to a display id

  - ### Improvements

    - Hide temporary files server if used
    - Pospone lauch of temporary files server till first kql command (support kqlmagic pre load)

  - ### Fix

    - Enables multiple notebook in parallel share temp files (now use kernel_id)
    - accurate detection of the jupypter application
    - fixed deep linking to Kusto explorer
  

## Version 0.1.110

  - ### Fix
  
    - Remove debug print messages

## Version 0.1.109

  - ### New popup_interaction options
  
    - defines mechanism to be used for popup interaction.
    - The valid options are "auto", "button", "reference", "webbrowser_open_at_kernel", "reference_popup"
    - When default "auto" is set, Kqlmagic choose based on the Jupyter front end used.


  - ### New Jupyter front end supported
  
    - Kqlmagic can be now used in **"azure data studio"** and **"visual studio code"**
    - Kqlmagic autodetect that it is running within Azure Data Studio
    - Kqlmagic will be pre installed in future Azure Data Studio version
    - Kqlmagic will be pre loaded in future Azure Data Studio version (no need to %reload_ext)


  - ### New plot package supported

    - **"plotly_widget"** was added to **plot_package** option (default **"plotly"**):
      - Generates the plot as widget image (useful to customize chart after plotted)
      - ```_kql_raw_result_.plotly_fig``` contains the rendered widget
      - all aspects of the plotted chart can be modified by modifying the plotly widget properties
      - plotly plot_package will be used instead of plotly_widget in case ipywidgets module is not found
      - ```%config Kqlmagic.plot_package=plotly_widget``` # sets the default plotly_package to plotly_widget
      - ```%kql -pp 'plotly_widget' ...``` # sets plotly_package to plotly_widget for thsi query

  - ### New feature - Kqlmagic can be used as a Module

    - Kqlmagic can be now used as a python Module (useful in environments that don't allow custom magics)
      - ```
        from Kqlmagic import kql   # import the kql function
        kql({kqlmagic line/cell text}) # execute the text as %kql / %% kql
        ```
      - the kql signature is: kql(text:str='', options:dict=None, query_properties:dict=None, vars:dict=None, conn:str=None, global_ns=None, local_ns=None)
        - options will override options parsed from text
        - query_properties will override query_properties parsed from text
        - conn will override default current connection or connection string parsed from text
        - vars will override python variables used to parametrized the query
        - global_ns and local_ns will override user namespace as derived from shell (not recommended, rare use)


  - ### New feature - support Jupyter display_id
  
      - When display_id is set to True, refresh will override the original chart
      - ```%kql -did ...``` # display the table or rendered chart linked to a display id
      - ```_kql_raw_result_.refresh() # will override the original chart```

  - ### New feature - refresh and submit functions support override_options, override_query_properties, override_vars and override_connection

    - ```_kql_raw_result_.refresh(override_options=options_dict)```  # will refresh the original query, using original options overriden by the override options.
    - ```_kql_raw_result_.refresh(override_query_properties=query_properties_dict)```      # will refresh the original query, using original query properties overriden by the override query properties.
    - ```_kql_raw_result_.refresh(override_vars=vars_dict)```      # will refresh the original parametrised query, using python vars overriden by the override vars.
    - ```_kql_raw_result_.refresh(override_connection=conn_str)```       # will refresh the original query, but to database as specified in the override connection string.


  - ### Fix
  
    - Fix parameterizer to better handle strings and to also handle pandas Series as a list
    - Use repr in parameterizer to safely quote string


## Version 0.1.108

  - ### New help information

    - help on how to enable logging. try ```%kql --help "logging"```
  
  - ### Fix
  
    - Fixed KQLMAGIC_CONNECTION_STR missing options bug
    - Fixed missing Orca bug
    - Fixed deep linking bug
    - Fixed faq display bug

## Version 0.1.107

  - ### New feature - request headers tagging
  
    - Enables to tag **x-ms-app**, **x-ms-user** and **x-ms-client-request-id** request headers with a custom string.
    - To get more information execute: ```%kql --help 'request-tags'```

  - ### New help information

    - help on how to tag request headers. try ```%kql --help "request-tags"```
    - Started a FAQ page. try %kql --faq


## Version 0.1.106

  - ### New support for Azure Data Explorer Client Request Properties

    - Many client request properties can be set using the set operator, as part of the query.
    However, some properties can be set only in the request.
    - To set client request properties in the request, use the same syntax as kqlmagic options, but instead of using the '-' prefix use the '+' as prefix.
      - example: ```%kql +servertimeout='30m' {your-query}```
    - Client request properties can also be set by using the query_properties option. It should be set as a dictionary with the properties values.
  
    - to see the full list of the client request properties try ```%kql --help "client-request-properties"```

  - ### New help information

    - help on how to use Kqlmagic behind proxies. try ```%kql --help "proxies"```
    - help on how to use Client Request Properties, and properties list. try ```%kql --help "client-request-properties"```
    - help on installing Kqlmagic. try ```%kql --help "kqlmagic-install"```
    - help to quick access Kqlmagic source. opens Kqlmagic github. try ```%kql --help "kqlmagic-github"```
    - help to quick access Kqlmagic readme. Opens Kqlmagic readme file. try ```%kql --help "kqlmagic-readme"```
    - help to quick access Kqlmagic license. Opens Kqlmagic license file. try ```%kql --help "kqlmagic-license"```
    - help to quick access Kqlmagic contributors list. Opens Kqlmagic contributors file. try ```%kql --help "kqlmagic-contributors"```

  - ### Fix

    - allow file names with spaces

  
## Version 0.1.105

  - ### New device_code login notification options
  
    - device_code login notification for device_code authentication.
    - Notification can be sent to 'frontend', 'terminal', 'browser' and 'email'
    - note: the 'browser' will open on the ipykernel server.
    - note: for email option to work, device_code_notification_email option have to be set too.

      - **device_code_login_notification** (default: **"frontend"**, Abbreviation: dcln)
        - Specifies device_code login notification method
        - **"frontend"** - displays a message with device code, and a button that when clicked opens an authentication page in the frontend browser.
        - **"terminal"** - displays a message with device code and link to the authentication page.
        - **"browser"** - displays a message with device code, and open an authentication page in a webbrowse on the ipykernel host.
        - **"email"** - send an email with device code and link to the authencitaion page.

      - **device_code_notification_email** (default: **''**, Abbreviation: dcne)
        - Email details string. initialized by environmental variable KQLMAGIC_DEVICE_CODE_NOTIFICATION_EMAIL.
        - The email details string format: SMTPEndPoint='endpoint';SMTPPort='port';sendFrom='email';sendFromPassword='password';sendTo='email';context='text'
        - note: context text is optional, is a free text that will be added to the email subject and email body.


  - ### New deep_link method in result object

    - deep_link method opens query link tool provided as a parameter or the default tool as set in query_link_destination option, and execute the query in the tool.
    - note: see **show_query_link** and **query_link_destination** options
    - note: supported only for Azure Data Explorer queries, will be ignored for Application Insights or Log Analytics queries
    - for example:
  
    ```_kql_raw_result_.deep_link()``` # will launch the default deep link tool and execute the query in the tool.

    ```_kql_raw_result_.deep_link("Kusto.WebExplorer")``` # will launch Kusto.WebExplorer and execute the query in Kusto.WebExplorer.

    ```_kql_raw_result_.deep_link("Kusto.Explorer")``` # will launch Kusto.Explorer and execute the query in Kusto.Explorer.


  - ### query errors displayed in pretty json

    - Show query errors in pretty json for better read.



## Version 0.1.104

  - Fix import missing FernetCrypto



## Version 0.1.103

  - ### New message appended to init banner

    - A what's new message with a 'what' new' button.
      - When clicked a window is opened with all Kqlmagic release notes history.


  - ### New init options (on Kqlmagic load) that can be set by environment variable KQLMAGIC_CONFIGURATION
  
    - **check_magic_version** (default **True**):
      - On Kqlmagic load, check whether new version of Kqlmagic exist, and add a notification message to init banner if a new version exist
  
    - **show_what_new** (default **True**):
      - On Kqlmagic load, loads history file of Kqlmagic from repository, and adds a "what's new" button to open it in a window.

    - **show_init_banner** (default **True**):
      - On Kqlmagic load, show init banner


  - ### New query options
  
    - **show_query_link** (default **False**, abbreviation sql):
      - Add query deep link button after query result, clicking the button will open the tool (see query_link_destination option) and execute the query in the tool.
        - note: supported only for Azure Data Explorer queries, will be ignored for Application Insights or Log Analytics queries
  
    - **query_link_destination**  (default **"Kusto.WebExplorer"**, abbreviation qld):
      - Specifies the destination of the query link tool (see show_query_link option)
        - note: only two Azure Data Explorer tools are supported **"Kusto.WebExplorer"** and **"Kusto.Explorer"**


  - ### New plot package supported
  
    - **"plotly_orca"** was added to **plot_package** option (default **"plotly"**):
      - Generates the plot as png image (useful for static front ends)
      - note: requires plotly orca to be installed in the backend server.

    - **"None"** was added to **plot_package** option (default **"plotly"**):
      - disable plot, table is returned



## Version 0.1.102

  - ### New query options

    - **show_query** (default False, abbreviation sq):

      - Add parametrized query message before query results.



## Version 0.1.101

  - ### support Souvereign Clouds

    - Supports **"public"**, **"mooncake"**, **"fairfax"**, **"blackforest"**, **"usnat"**, **"ussec"**, **"test"** clouds.
  

  - ### New query options

    - **cloud**  (default **"public"**, other: [ **"public"**, **"mooncake"**, **"fairfax"**, **"blackforest"**, **"usnat"**, **"ussec"**, **"test"**]):
      - Specifies the cloud to be used when a new connection is created

    - **data-source-url**
      - Override the cloud default data source url
  
    - **aad-url**
      - Override the cloud default aad login url
        - note: only two Azure Data Explorer tools are supported **"Kusto.WebExplorer"** and **"Kusto.Explorer"**


  - ### Connection string enhanced

    - **clientId**
      - Client id can be specified in all connection string variation to override the default clientId





## Version <= 0.1.100

- Fixed nbsp; bug + allow multiple options in connection string Aug 12, 2019
- Adding support to plotly version 4 Jul 21, 2019
- Adding another test Jul 21, 2019
- Changed testing framework from nose to pytest Jun 3, 2019
- Update setup module versions Jun 3, 2019
- Version 0.96 - added queryproperties option and "+" specific query properties for Kusto queries (supports all set customers properties) 
- fix query caching  to support non standard database names; support separate logging; log Kqlmagic steps; log requests; log adal request; add env KQLMAGIC [-LOG_LEVEL, _LOG_FILE, _LOG_FILE_PREFIX, _LOG_FILE_MODE]; add experimental sso, to enable KQLMAGIC_ENABLE_SSO=TRUE; fix parametrized dataframe types long and real; - 0.1.95 - Apr 06, 2019
- fix http://kusto.aria.microsoft.com and other not standard cluster name 
- fix missing lxml.py module dependency in setup.py; created history.rst file; updated setup.py based on azure notebooks image - 0.1.93 - Mar 24, 2019
- support database names with whitespaces; support ADX proxy over AI/LA cluster names; fix charts based on firest quantity column - 0.1.92 - Mar 18, 2019
- allow v1 response from kusto api v2, support adx-proxy, - 0.1.91 - Feb 20, 2019
- fix to_dataframe bool - 0.1.90 - Jan 25, 2019
- update binder requirements to 0.1.88 - Jan 16, 2019
- version 01.88, added --schema command - 0.1.88 - Jan 16, 2019
- added test_notebook_app for testing, , commands instead of show_window return html object result. - 0.1.87 - Dec 25, 2018
- added run_upgrade.bat; enhanced run_tests.bat - Dec 24, 2018
- fixed run_tests.bat - fixed run_tests.bat - Dec 20, 2018
- update binder requirements - update binder requirements - Dec 20, 2018
- Adjusted to support VisualStudioCode (Jupyter) notebook - 0.1.86 - Dec 18, 2018
- added support to "ipython" notebook_app, started a tests suits - 0.1.85 - Dec 17, 2018
- update binder requirements to 0.1.84 - Dec 16, 2018
- Fix dynamic columns that are not strict json - 0.1.84 - Dec 11, 2018
- Align to Kusto new V2 dynamic field response; fix kql.submit() - 0.1.83 - Dec 10, 2018
- binder to use 0.1.82 - Dec 9, 2018
- added binder folder, with requirements - Dec 9, 2018
- fixed env KQLMAGIC_CONNECTION_STR - 0.1.82 - Dec 6, 2018
- update notebooks with anonymous authentication - Dec 3, 2018
- removed tell_format; added anonymous authentication (for the case of local data source) - 0.1.81 - Dec 3, 2018
- Removed dependency on azure.kusto sdk, use rest api to kusto. - 0.1.80 - Nov 13, 2018
- changed cache management via commands, modified caches to be named, added option -save_to folder,; support datetime as linear value in charts, support kql render attributes; support fully qualified cluster; prepare to remove kusto sdk - 0.1.79 - Nov 13, 2018
- support query option -timout / -wait / -to in seconds - 0.1.78 - Nov 8, 2018
- fix popup for clusters or databases with special characters, fix .ingest online, version - 0.1.77 - Nov 8, 2018
- restrict Kqlmagic to python >= 3.6 - 0.1.76 - Nov 8, 2018
- support command --help without params - 0.1.75 - Nov 7, 2018
- version 0.1.74 - 0.1.74 - Nov 7, 2018
- support .show database DB schema as json - Nov 7, 2018
- fix parametrization of df column of type object but is actually bytes 0.1.73 - 0.1.73 - Nov 5, 2018
- fixed parametrizaton to .set management queries, fixed javascript error when old out cell displays - 0.1.72 - Nov 5, 2018
- prepare support all visualization properties - 0.1.71 - Nov 3, 2018
- minor changes in readme and help - Oct 31, 2018
- support pandas dataframe as parameter, add support null values in conversion to dataframes, fixed pretty print of dynamic cols; improved parametrization. - 0.1.70 - Oct 31, 2018
- make keys as caseinsensitive, ignore underscore and hyphen-minus, covert some options to commands, modify kusto kql logo, and remove kusto name. 0.1.69 - 0.1.69 - Oct 29, 2018
- fix clors notebook - Oct 25, 2018
- popup_window option to all commands, fixed banner, update notebooks, 0.1.68 - 0.1.68 - Oct 25, 2018
- update notebooks with --version,; allow =setting in options, more quotes flexibility with values, support option dict type; - 0.1.67 - Oct 25, 2018
- support partial result, add command concept, added commands, 0.1.66 - 0.1.66 - Oct 25, 2018
- options and connection key values can be parametyrize from python and env variables, new -query and -conn option - 0.1.65 - Oct 23, 2018
- parametrized options, add file://folder, update color notebook, 0.1.64 - 0.1.64 - Oct 22, 2018
- fix notebooks - Oct 22, 2018
- run black on code, version 0.1.63 - 0.1.63 - Oct 22, 2018
- bug fix, code refactor, version 0.1.62 - 0.1.62 - Oct 21, 2018
- moved the saved_as earlier in the pipe, to capture raw results even if there is an error later. version 0.1.61 - 0.1.61 - Oct 20, 2018
- fix to pandas convertion, for the case of missing int64 or missing bool, version 0.1.60 - 0.1.60 - Oct 18, 2018
- update notebooks, added support to certificate pem_file, version 0.1.59 - 0.1.59 - Oct 18, 2018
- restructure local files - Oct 18, 2018
- Update notebooks, created QuikStart fro log analytics notebook, updated README, version 0.1.58 - 0.1.58 - Oct 18, 2018
- update notebooks - Oct 18, 2018
- update notebooks - Oct 18, 2018
- update notebooks with new connection string format - Oct 18, 2018
- Fixed setup.py long description to show properly work on PyPI - 0.1.57 -  Oct 17, 2018
- removed setup dwonload_url - Oct 17, 2018
- update setup description and README titles - Oct 17, 2018
- update README - Oct 17, 2018
- update README - Oct 17, 2018
- update README - Oct 17, 2018
- updated README, and setup - 0.1.56 - Oct 17, 2018
- fix setup.py - fix setup.py
- removed version restriction on traitlets - 0.1.55 - Oct 17, 2018
- removed psutil version restriction in setup - 0.1.54 - Oct 17, 2018
- fixed pallette notebook; adjuset to azure-kusto-data version 0.0.15 changes (tenant is a must parameter, for clienid) - 0.1.53 - Oct 16, 2018
- Fixed development status, updated setup, version 0.1.52 - 0.1.52 - Oct 16, 2018
- changes state from 1-alpha to 3-beta; published in PyPI; modified notebooks to reflect the changes; changed structure of files. Version 0.1.50 - 0.1.50 -  Oct 16, 2018
- support alt schema names; simplify code; improve connection inheritance; ; added warning to upgrade version with PyPI; added alias to connection string; added alias_magics 'adxql'; - 0.1.47 - Oct 15, 2018
- removed order restriction on connection string, simplified and unified connections string code parsing, improved error presentation, added friendly_ to connection string (for case id is a uuid) - 0.1.46 - Oct 10, 2018
- AAD Authentication Code() and Clientid/Clientsecret, added to ApplicationInsights and Loganalytics - 0.1.45 - Oct 9, 2018
- add aad auth support for lognalytics and applicationinsights; simplified code - 0.1.44 - Oct 8, 2018
- support getting generic database schema from all engines - 0.1.43 - Oct 7, 2018
- fixed fork charts, adjusted to work on Jupyter Lab - 0.1.42 - Oct 7, 2018
- fixed linechart and timechart multidimetional and not sorted, and not ordered, 0.1.41 - 0.1.41 - Oct 4, 2018
- bump version 0.1.40
- bump 0.1.39
- fix results.py -  Oct 3, 2018
- bump version 0.1.38
- patch to support plotly in Azure notebook, till it will support plotly MIME - Oct 3, 2018
- comment local usage - Oct 3, 2018
- add ParametrizeYourQuery notebook - Oct 3, 2018
- added parametrization feature, added saved_as option, added params_dict option, fixed options validation, added parametrized_query attribute to results object, updated setup.py - 0.1.37 - Sep 30, 2018
- simplified code, fixed cahing schema - 0.1.36 - Sep 24, 2018
- Fixed timespan - 0.1.35 - Sep 23, 2018
- fixed ref to github - Sep 20, 2018
- Added copyright file header to all files, version 0.1.34 - 0.1.34 - Sep 20, 2018
- fix notebooks - 0.1.33 - Sep 19, 2018
- fix setup.py - 0.1.32 - Sep 19, 2018
- bump version - 0.1.31 - Sep 19, 2018

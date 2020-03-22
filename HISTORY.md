# HISTORY

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
      - *_kql_raw_result_.plotly_fig contains the rendered widget
      - all aspects of the plotted chart can be modified by modifying the plotly widget properties
      - plotly plot_package will be used instead of plotly_widget in case ipywidgets module is not found
      - *$config Kqlmagic.plot_package=plotly_widget* # sets the default plotly_package to plotly_widget
      - *%kql -pp 'plotly_widget' ...* # sets plotly_package to plotly_widget for thsi query

  - ### New feature - Kqlmagic can be used as a Module

    - Kqlmagic can be now used as a python Module (useful in environments that don't allow custom magics)
      - *from Kqlmagic import kql*    # import the kql function
      - *kql({kqlmagic line/cell text}) # execute the text as %kql / %% kql
      - the kql signature is: kql(text:str='', options:dict=None, query_properties:dict=None, vars:dict=None, conn:str=None, global_ns=None, local_ns=None)
        - options will override options parsed from text
        - query_properties will override query_properties parsed from text
        - conn will override default current connection or connection string parsed from text
        - vars will override python variables used to parametrized the query
        - global_ns and local_ns will override user namespace as derived from shell (not recommended, rare use)


  - ### New feature - support Jupyter display_id
  
      - When display_id is set to True, refresh will override the original chart
      - *%kql -did ...* # display the rendered chart bundked to a display id
      - *_kql_raw_result_.refresh() # will override the original chart

  - ### New feature - refresh and submit functions support override_options, override_query_properties, override_vars and override_connection

    - *_kql_raw_result_.refresh(override_options=options_dict)*       # will refresh the original query, using original options overriden by the override options.
    - ipywidgetsrefresh(override_query_properties=query_properties_dict)*       # will refresh the original query, using original query properties overriden by the override query properties.
    - *_kql_raw_result_.refresh(override_vars=vars_dict)*       # will refresh the original parametrised query, using python vars overriden by the override vars.
    - *_kql_raw_result_.refresh(override_connection=conn_str)*       # will refresh the original query, but to database as specified in the override connection string.


  - ### Fix
  
    - Fix parameterizer to better handle strings and to also handle pandas Series as a list
    - Use repr in parameterizer to safely quote string


## Version 0.1.108

  - ### New help information

    - help on how to enable logging. try %kql --help "logging"
  
  - ### Fix
  
    - Fixed KQLMAGIC_CONNECTION_STR missing options bug
    - Fixed missing Orca bug
    - Fixed deep linking bug
    - Fixed faq display bug

## Version 0.1.107

  - ### New feature - request headers tagging
  
    - Enables to tag **x-ms-app**, **x-ms-user** and **x-ms-client-request-id** request headers with a custom string.
    - To get more information execute: ```%kql --help 'request-tags'

  - ### New help information

    - help on how to tag request headers. try %kql --help "request-tags"
    - Started a FAQ page. try %kql --faq


## Version 0.1.106

  - ### New support for Azure Data Explorer Client Request Properties

    - Many client request properties can be set using the set operator, as part of the query.
    However, some properties can be set only in the request.
    - To set client request properties in the request, use the same syntax as kqlmagic options, but instead of using the '-' prefix use the '+' as prefix.
      - example: %kql +servertimeout='30m' {your-query}
    - Client request properties can also be set by using the query_properties option. It should be set as a dictionary with the properties values.
  
    - to see the full list of the client request properties try %kql --help "client-request-properties"

  - ### New help information

    - help on how to use Kqlmagic behind proxies. try %kql --help "proxies"
    - help on how to use Client Request Properties, and properties list. try %kql --help "client-request-properties"
    - help on installing Kqlmagic. try %kql --help "kqlmagic-install"
    - help to quick access Kqlmagic source. opens Kqlmagic github. try %kql --help "kqlmagic-github"
    - help to quick access Kqlmagic readme. Opens Kqlmagic readme file. try %kql --help "kqlmagic-readme"
    - help to quick access Kqlmagic license. Opens Kqlmagic license file. try %kql --help "kqlmagic-license"
    - help to quick access Kqlmagic contributors list. Opens Kqlmagic contributors file. try %kql --help "kqlmagic-contributors"

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
  
    *_kql_raw_result_.deep_link()*                    # will launch the default deep link tool and execute the query in the tool.

    *_kql_raw_result_.deep_link("Kusto.WebExplorer")* # will launch Kusto.WebExplorer and execute the query in Kusto.WebExplorer.

    *_kql_raw_result_.deep_link("Kusto.Explorer")*    # will launch Kusto.Explorer and execute the query in Kusto.Explorer.


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

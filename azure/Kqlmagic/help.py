# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

"""A module that manage package version.
"""


from .dependencies import Dependencies
from .constants import Constants, Email, SsoEnvVarParam


_KQL_URL                   = "http://aka.ms/kdocs"
_APPINSIGHTS_URL           = "https://docs.microsoft.com/en-us/azure/application-insights/app-insights-overview?toc=/azure/azure-monitor/toc.json"
_LOGANALYTICS_URL          = "https://docs.microsoft.com/en-us/azure/log-analytics/log-analytics-queries?toc=/azure/azure-monitor/toc.json"
_AZUREMONITOR_URL          = "https://docs.microsoft.com/en-us/azure/azure-monitor/"
_KUSTO_URL                 = "https://docs.microsoft.com/en-us/azure/data-explorer/"
_KQLMAGIC_DOWNLOADS_URL    = "https://pepy.tech/project/Kqlmagic"
_KQLMAGIC_INSTALL_URL      = "https://pypi.org/project/Kqlmagic/"
_KQLMAGIC_README_URL       = "https://github.com/microsoft/jupyter-Kqlmagic/blob/master/README.md"
_KQLMAGIC_GITHUB_URL       = "https://github.com/microsoft/jupyter-Kqlmagic"
_KQLMAGIC_LICENSE_URL      = "https://github.com/microsoft/jupyter-Kqlmagic/blob/master/LICENSE.TXT"
_KQLMAGIC_CONTRIBUTORS_URL = "https://github.com/microsoft/jupyter-Kqlmagic/blob/master/CONTRIBUTORS.md"
_KQLMAGIC_FAQ_URL          = "https://raw.githubusercontent.com/microsoft/jupyter-Kqlmagic/master/FAQ.html"

_NEED_SUPPORT_SECTION = """## Need Support?
- **Have a feature request for Kqlmagic?** Please post it on [User Voice](https://feedback.azure.com/forums/913690-azure-monitor) to help us prioritize
- **Have a technical question?** Ask on [Stack Overflow with tag "Kqlmagic"](https://stackoverflow.com/questions/tagged/Kqlmagic)
- **Need Support?** Every customer with an active Azure subscription has access to [support](https://docs.microsoft.com/en-us/azure/azure-supportability/how-to-create-azure-support-request) with guaranteed response time.  Consider submitting a ticket and get assistance from Microsoft support team
- **Found a bug?** Please help us fix it by thoroughly documenting it and [filing an issue](https://github.com/Microsoft/jupyter-Kqlmagic/issues/new).
"""


_HELP_OPTIONS = ""


_HELP_COMMANDS = """## Overview
Except submitting kql queries, few other commands are included that may help using the Kqlmagic.<br>
- Only one command can be executed per magic transaction.<br>
- A command must start with a double hyphen-minus ```--```<br>
- If command is not specified, the default command ```"submit"``` is assumed, that submits the query.<br>

## Commands
The following commands are supported:<br>
- **submit** - Execute the query and return result. <br>
    - Options can be used to customize the behavior of the transaction.<br>
    - The query can parametrized.<br>
    - This is the default command.<br>
<br>

- **version** - Displays Kqlmagic current version string.<br>
<br>

- **bug_report** - gathers last cell execution state, ready to be copy/paste to Kqlmagic github issues (you must have an account in github)<br>
      - Kqlmagic version<br>
      - platform information<br>
      - python information<br>
      - Kqlmagic dependencies version <br>
      - Kqlmagic default options<br>
      - Kqlmagic connections<br>
      - Kqlmagic environment variables<br>
      - Last kql execution context (options, parameters, tags, request, response), including error stack in case it failed<br>
<br>

- **py** - execute python code in user's namespace, code may change namespace state, and if last statement is an expression it returns value.<br>
<br>

- **pyrw** - execute python code in user's namespace, code may change namespace state, and if last statement is an expression it returns value.<br>
<br>

- **pyro** - execute python code in user's namespace, namespace state does NOT change, and if last statement is an expression it returns value.<br>
<br>

- **line_magic** - executes a python kernel line magic.<br>
    - Only this line will be input to the specified line magic<br>
    - "%" syntax can be used instaed of the command name<br>
<br>

- **cell_magic** - executes a python kernel cell magic.<br>
    - All following lines will be input to the specified cell magic<br>
    - "%%" syntax can be used instead of the command name<br>
<br>

- **activate_kernel** - activates kqlmagic kernel over python kernel.<br>
    - After activation, code cells executes by default as kqlmagic cells<br>
<br>

- **deactivate_kernel** - deactivates kqlmagic kernel over python kernel (switch back to python kernel).<br>
<br>

- **cache** - enables/disables caching query results to the specified folder.<br>
    - OPTIONAL - Stored cached data can be also queried using the cache schema: cache://<cache-folder-name>
<br>

- **use_cache** - enables/disables using query results stored in specified folder as cache.<br>
    - OPTIONAL - Stored cached data can be also queried using the cache schema: cache://<cache-folder-name>
<br>

- **conn** - Displays connections info.<br>
<br>

- **banner** - Displays Kqlmagic init banner.<br>
<br>

- **usage** - Displays usage of Kqlmagic.<br>
<br>

- **config** - get/set Kqlmagic default option<br>
<br>

- **faq** - Displays Frequently Asked Questions on Kqlmagic.<br>
<br>

- **help "topic"** - Displays information about the topic.<br>
    - To get the list of all the topics, execute ```%kql --help "help"```<br>
<br>

- **palette - Display information about the current or other named color palette.<br>
    - The behaviour of this command will change based on the specified option:
    - -palette_name, -palette_colors, palette_reverse, -palette_desaturation, execute ```%kql --palette -palette_name "Reds"```<br>
<br>

- **palettes - Display information about all available palettes.<br>
    - The behaviour of this command will change based on the specified option:
    - -palette_colors, palette_reverse, -palette_desaturation, execute ```%kql --palettes -palette_desaturation 0.75```<br>
<br>

- **schema "database"** - Returns the database schema as a python dict (displayed as a json format). <br>
    - To get Azure Data Explorer database schema: ```%kql --schema "databasename@clustername"```<br>
    - To get application insights app schema: ```%kql --schema "appname@applicationinsights"```<br>
    - To get log analytics workspace schema: ```%kql --schema "workspacename@loganalytics"```<br>
    - To get current connection database schema ```%kql --schema```<br>
    - If -conn option is sepcified it will override the database value.<br>
<br>

- **cache - Enables caching query results to a cache folder, or disbale. <br>
    - To enable caching to folder XXX, execute: ```%kql --cache "XXX"```<br>
    - To disable caching, execute: ```%kql --cache None```<br>
    - Once results are cached, the results can be used by enabling the use of the cache, with the --use_cache command.<br>
<br>

- **use_cache - Enables use of cached results from a cache folder. <br>
    - To enable use of cache from folder XXX, execute: ```%kql --use_cache "XXX"```<br>
    - To disable use of cache, execute: ```%kql --use_cache None```<br>
    - Once enabled, intead of quering the data source, the results are retreived from the cache.<br>
<br>

## Examples:
```%kql --version```<br><br>
```%kql --banner```<br><br>
```%kql --usage```<br><br>
```%kql --config "show_query_time"```<br><br>
```%kql --config "show_query_time = True"```<br><br>
```%kql --config```<br><br>
```%kql --faq```<br><br>
```%kql --help "help"```<br><br>
```%kql --help "options"```<br><br>
```%kql --help "conn"```<br><br>
```%kql --palette -palette_name "Reds"```<br><br>
```%kql --schema 'DEMO_APP@applicationinsights'```<br><br>
```%kql --cache "XXX"```<br><br>
```%kql --use_cache None```<br><br>
```%kql --submit appinsights://appid='DEMO_APP';appkey='DEMO_KEY' pageViews | count```<br><br>
```%kql --palettes -palette_desaturation 0.75```
```%kql pageViews | count```
"""


_USAGE = f"""## Usage:
**line usage:** ```%kql [command] [conn] [result <<] [options] [query]```

**cell usage:** ```%%kql [conn] [result <<] [options] [query] [[EMPTY-LINE [result <<] [options]]*```<br>

- **command** - The command to be executed. 
    - If not specified the query will be submited, if exist.<br>
    - All commands start with a double hyphen-minus ```--```<br>
    - To get more information, execute ```%kql --help "commands"```<br>
<br>

- **conn** - Connection string or reference to a connection to Azure Monitor resource.<br>
    - If not specified the current (last created or used) connection will be used.<br>
    - The conncan be also specified in the options parts, using the option ```-conn```<br>
    - To get more information, execute ```%kql --help "conn"```<br>
<br>

- **result** - Python variable name that will be assigned with the result of the query.<br>
    - Query results are always assigned to ```_``` and to ```_kql_raw_result_``` python variables.<br>
    - If not specified and is last query in the cell, the results will be displayed.<br>
<br>

- **options** - Options that customize the behavior of the Kqlmagic for this transaction only.<br>
    - All options start with a hyphen-minus ```-```<br>
    - To get more information, execute ```%kql --help "options"```<br>
<br>

- **query** - Kusto Query language (kql) query that will be submited to the specified connection or to the current connection.<br>
    - The query can be also sepcified in the options parts, using the option ```-query```<br>
    - To get more information, browse https://docs.microsoft.com/en-us/azure/kusto/query/index 
<br>

## Examples:
```%kql --version```<br><br>
```%kql --usage```<br><br>
```%%kql appinsights://appid='DEMO_APP';appkey='DEMO_KEY' 
    pageViews 
    | where client_City != '' 
    | summarize count() by client_City 
    | sort by count_ 
    | limit 10```<br><br>
```%%kql pageViews | where client_City != '' 
    | summarize count() by client_City | sort by count_ 
    | limit 10```<br><br>
```%kql DEMO_APP@appinsights pageViews | count```

## Get Started Notebooks:

* [Get Started with Kqlmagic for Kusto](https://mybinder.org/v2/gh/Microsoft/jupyter-Kqlmagic/master?filepath=notebooks%2FQuickStart.ipynb)

* [Get Started with Kqlmagic for Application Insights](https://mybinder.org/v2/gh/Microsoft/jupyter-Kqlmagic/master?filepath=notebooks%2FQuickStartAI.ipynb)

* [Get Started with Kqlmagic for Log Analytics](https://mybinder.org/v2/gh/Microsoft/jupyter-Kqlmagic/master?filepath=notebooks%2FQuickStartLA.ipynb)


* [Parametrize your Kqlmagic query with Python](https://mybinder.org/v2/gh/Microsoft/jupyter-Kqlmagic/master?filepath=notebooks%2FParametrizeYourQuery.ipynb)

* [Choose colors palette for your Kqlmagic query chart result](https://mybinder.org/v2/gh/Microsoft/jupyter-Kqlmagic/master?filepath=notebooks%2FColorYourCharts.ipynb)

{_NEED_SUPPORT_SECTION}"""


_HELP_HELP = f"""## Overview
Help command is a tool to get more information on a topics that are relevant to Kqlmagic.<br>help

usage: ```%kql --help "topic"```<br>

## Topics
- **usage** - How to use Kqlmagic.<br>
<br>

- **config** - Lists Kqlmagic default options. The same as --config without parameters.<br>
<br>

- **faq** - Reference to Kqlmagic FAQ<br>
<br>

- **conn** - Lists the available connection string variation, and how their are used to authenticatie to data sources.<br>
<br>

- **query** / **kql** - [Reference to resources Kusto Query language, aka kql, documentation]({_KQL_URL})<br>
<br>

- **options** - Lists the available options, and their behavior impact on the submit query command.<br>
<br>

- **env** - Lists all {Constants.MAGIC_CLASS_NAME_UPPER} environment variables and their purpose.<br>
<br>

- **commands** - Lists the available commands, and what they do.<br>
<br>

- **proxies** - How to use Kqlmagic via proxies.<br>
<br>

- **sql** - Azure Data Explorer SQL support.<br>
<br>

- **aria** - How to query Aria's Azure Data Explorer databases.<br>
<br>

- **client-request-properties** - How to use Client Request properties, and properties list.<br>
<br>

- **request-tags** - How to tag request headers.<br>
<br>

- **AzureMonitor**- [Reference to resources Azure Monitor tools]({_AZUREMONITOR_URL})<br>
Azure Monitor, which now includes Log Analytics and Application Insights, provides sophisticated tools for collecting and analyzing telemetry that allow you to maximize the performance and availability of your cloud and on-premises resources and applications. It helps you understand how your applications are performing and proactively identifies issues affecting them and the resources they depend on.<br>
<br>

- **AzureDataExplorer** / **kusto**- [Reference to resources Azure Data Explorer (kusto) service]({_KUSTO_URL})<br>
Azure Data Explorer is a fast and highly scalable data exploration service for log and telemetry data. It helps you handle the many data streams emitted by modern software, so you can collect, store, and analyze data. Azure Data Explorer is ideal for analyzing large volumes of diverse data from any data source, such as websites, applications, IoT devices, and more.<br>
<br>

- **LogAnalytics**- [Reference to resources Log Analytics service]({_LOGANALYTICS_URL})<br>
Log data collected by Azure Monitor is stored in Log Analytics which collects telemetry and other data from a variety of sources and provides a query language for advanced analytics.<br>
<br>

- **ApplicationInsights** / **AppInsights**- [Reference to resources Application Insights service]({_APPINSIGHTS_URL})<br>
Application Insights is an extensible Application Performance Management (APM) service for web developers on multiple platforms. Use it to monitor your live web application. It will automatically detect performance anomalies. It includes powerful analytics tools to help you diagnose issues and to understand what users actually do with your app. It's designed to help you continuously improve performance and usability. It works for apps on a wide variety of platforms including .NET, Node.js and J2EE, hosted on-premises or in the cloud. It integrates with your DevOps process, and has connection points to a variety of development tools. It can monitor and analyze telemetry from mobile apps by integrating with Visual Studio App Center.<br>
<br>

- **kqlmagic-readme** - [Reference to Kqlmagic readme]({_KQLMAGIC_README_URL})<br>
<br>

- **kqlmagic-github** - [Reference to Kqlmagic github]({_KQLMAGIC_GITHUB_URL})<br>
<br>

- **kqlmagic-license** - [Reference to Kqlmagic license]({_KQLMAGIC_LICENSE_URL})<br>
<br>

- **kqlmagic-contributors** - [Reference to Kqlmagic contributors]({_KQLMAGIC_CONTRIBUTORS_URL})<br>
<br>

- **kqlmagic-install** - [Reference to Kqlmagic pypi install readme]({_KQLMAGIC_INSTALL_URL})<br>
<br>

- **kqlmagic-downloads** - [Reference to Kqlmagic downloads data]({_KQLMAGIC_DOWNLOADS_URL})<br>
<br>

- **logging** - How to enable logging.<br>
<br>

- **help** - This help.<br>
<br>

{_NEED_SUPPORT_SECTION}"""


_HELP_CONN = f"""## Overview
- To get data from Azure Monitor data resources, the user need to authenticate itself, and if it has the right permission, 
he would be able to query that data resource.
- The current supported data sources are: Azure Data Explorer (kusto) clusters, Application Insights, Log Analytics and Cache.
- Cache data source is not a real data source, it retrieves query results that were cached, but it can only retreive results queries that were executed before, new queries or modified queries won't work.
to get more information on cache data source, execute ```help "cache"```

- The user can connect to multiple data resources.
- Once a connection to a data resource is established, it gets a name of the form <resource>@<data-source>.
- Reference to a data resource can be by connection string, connection name, or current connection (last connection used).
    - If connection is not specified, current connection (last connection used) will be used.
    - To submit queries, at least one connection to a data resource must be established.

- When a connection is specified, and it is a new connection string, the authentication and authorization is validated authomatically, by submiting 
a validation query ```range c from 1 to 10 step 1 | count```, and if the correct result returns, the connection is established.

- An initial connection can be specified as an environment variable.
    - if specified it will be established when Kqlmagic loads.
    - The variable name is ```KQLMAGIC_CONNECTION_STR```

## Authentication methods:

* AAD Username/password - Provide your AAD username and password.
* AAD application - Provide your AAD tenant ID, AAD app ID and app secret.
* AAD code - Provide only your AAD username, and authenticate yourself using a code, generated by ADAL.
* certificate - Provide your AAD tenant ID, AAD app ID, certificate and certificate-thumbprint (supported only with Azure Data Explorer)
* appid/appkey - Provide you application insight appid, and appkey (supported only with Application Insights)
* anonymous - No authentication. For the case that you run your data source locally.

## Connect to Azure Data Explorer (kusto) data resource ```<database or alias>@<cluster>```
Few options to authenticate with Azure Data Explorer (Kusto) data resources:<br>
```%kql azuredataexplorer://code;cluster='<cluster-name>';database='<database-name>';alias='<database-friendly-name>'```<br><br>
```%kql azuredataexplorer://tenant='<tenant-id>';clientid='<aad-appid>';clientsecret='<aad-appkey>';cluster='<cluster-name>';database='<database-name>';alias='<database-friendly-name>'```<br><br>
```%kql azuredataexplorer://tenant='<tenant-id>';certificate='<certificate>';certificate_thumbprint='<thumbprint>';cluster='<cluster-name>';database='<database-name>';alias='<database-friendly-name>'```<br><br>
```%kql azuredataexplorer://tenant='<tenant-id>';certificate_pem_file='<pem_filename>';certificate_thumbprint='<thumbprint>';cluster='<cluster-name>';database='<database-name>';alias='<database-friendly-name>'```<br><br>
```%kql azuredataexplorer://username='<username>';password='<password>';cluster='<cluster-name>';database='<database-name>';alias='<database-friendly-name>'```<br><br>
```%kql azuredataexplorer://anonymous;cluster='<cluster-name>';database='<database-name>';alias='<database-friendly-name>'```<br><br>

Notes:<br>
- username/password works only on corporate network.<br>
- alias is optional.<br>
- if credentials are missing, and a previous connection was established the credentials will be inherited.<br>
- if secret (password / clientsecret / thumbprint) is missing, user will be prompted to provide it.<br>
- if cluster is missing, and a previous connection was established the cluster will be inherited.<br>
- if tenant is missing, and a previous connection was established the tenant will be inherited.<br>
- if only the database change, a new connection can be set as follow: 
```<new-database-name>@<cluster-name>```<br>
- **a not quoted value, is a python expression, that is evaluated and its result is used as the value. This is how you can parametrize the connection string** 

## Connect to Log Analytics data resources ```<workspace or alias>@loganalytics```
Few options to authenticate with Log Analytics:<br>
```%kql loganalytics://code;workspace='<workspace-id>';alias='<workspace-friendly-name>'```<br><br>
```%kql loganalytics://tenant='<tenant-id>';clientid='<aad-appid>';clientsecret='<aad-appkey>';workspace='<workspace-id>';alias='<workspace-friendly-name>'```<br><br>
```%kql loganalytics://username='<username>';password='<password>';workspace='<workspace-id>';alias='<workspace-friendly-name>'```<br><br>
```%kql loganalytics://anonymous;workspace='<workspace-id>';alias='<workspace-friendly-name>'```<br><br>

Notes:<br>
- authentication with appkey works only for the demo.<br>
- username/password works only on corporate network.<br>
- alias is optional.<br>
- if credentials are missing, and a previous connection was established the credentials will be inherited.<br>
- if secret (password / clientsecret) is missing, user will be prompted to provide it.<br>
- if tenant is missing, and a previous connection was established the tenant will be inherited.<br>
- **a not quoted value, is a python expression, that is evaluated and its result is used as the value. This is how you can parametrize the connection string**


## Connect to Application Insights data resources ```<appid or alias>@appinsights```
Few options to authenticate with Apllication Insights:<br><br>
```%kql appinsights://appid='<app-id>';appkey='<app-key>';alias='<appid-friendly-name>'```<br><br>
```%kql appinsights://code;appid='<app-id>';alias='<appid-friendly-name>'```<br><br>
```%kql appinsights://tenant='<tenant-id>';clientid='<aad-appid>';clientsecret='<aad-appkey>';appid='<app-id>';alias='<appid-friendly-name>'```<br><br>
```%kql appinsights://username='<username>';password='<password>';appid='<app-id>';alias='<appid-friendly-name>'```<br><br>
```%kql appinsights://anonymous;appid='<app-id>';alias='<appid-friendly-name>'```<br><br>

Notes:<br>
- username/password works only on corporate network.<br>
- alias is optional.<br>
- if credentials are missing, and a previous connection was established the credentials will be inherited.<br>
- if secret (password / clientsecret / appkey) is missing, user will be prompted to provide it.<br>
- if tenant is missing, and a previous connection was established the tenant will be inherited.<br>
- **a not quoted value, is a python expression, that is evaluated and its result is used as the value. This is how you can parametrize the connection string**

{_NEED_SUPPORT_SECTION}"""


_HELP_CACHE = ""

_HELP_LOGGING = f"""## Overview
- Logging is available, mainly for development debugging. To enable logging one or more of the below environment variable must be set.<br>
The log file is created in the same folder as the current notebook.<br>
<br>

## Logging varaibles
- **{Constants.MAGIC_CLASS_NAME_UPPER}_LOG_LEVEL**<br>
Log level. The following levels are supported: 'FATAL', 'ERROR', 'WARNING', 'INFO', and 'DEBUG'<br>
The default level is: 'DEBUG'<br>
<br>

- **{Constants.MAGIC_CLASS_NAME_UPPER}_LOG_FILE**<br>
Filename for the log messages.<br>
<br>

- **{Constants.MAGIC_CLASS_NAME_UPPER}_LOG_FILE_PREFIX**<br>
If filename is not specified, the filename will be build from the prefix as follows: '{{prefix}}-{{ipykernel-unique-key}}.log'<br>
The default preix is: '{Constants.MAGIC_PACKAGE_NAME}'<br>
<br>

- **{Constants.MAGIC_CLASS_NAME_UPPER}_LOG_FILE_MODE**<br>
The mode at which the filename is oppened. The following modes are supported: 'append' and 'write'<br>
The default mode is: 'write'<br>
<br>
"""

_HELP_ENV = f"""## Overview
you can configure {Constants.MAGIC_CLASS_NAME} using the following environment variables:

## {Constants.MAGIC_CLASS_NAME_UPPER}_LOG_LEVEL
Log level. The following levels are supported: 'FATAL', 'ERROR', 'WARNING', 'INFO', and 'DEBUG'<br>
Default: 'DEBUG' if a filename is specified, otherwise None<br>
<br>

## {Constants.MAGIC_CLASS_NAME_UPPER}_LOG_FILE
Filename for the log messages.<br>
Default: None<br>
<br>

## {Constants.MAGIC_CLASS_NAME_UPPER}_LOG_FILE_PREFIX
If {Constants.MAGIC_CLASS_NAME_UPPER}_LOG_FILE is not specified, the filename will be build from the prefix as follows: '{{prefix}}-{{ipykernel-unique-key}}.log'<br>
Default: '{Constants.MAGIC_PACKAGE_NAME}' if {Constants.MAGIC_CLASS_NAME_UPPER}_LOG_LEVEL is epcified, otherwise None<br>
<br>

## {Constants.MAGIC_CLASS_NAME_UPPER}_LOG_FILE_MODE
The mode at which the log filename is oppened. The following modes are supported: 'append' and 'write'<br>
Default: 'write' if a filename is specified, otherwise None<br>
<br>

## {Constants.MAGIC_CLASS_NAME_UPPER}_CONNECTION_STR
Set a default connection string. If set, the connection will be set on {Constants.MAGIC_PACKAGE_NAME} load.<br>
Default: None<br>
<br>

## {Constants.MAGIC_CLASS_NAME_UPPER}_SSO_ENCRYPTION_KEYS
Specifies details to enable SSO.<br>
Should be a list of key='value' pairs separated by a semicolon ';'<br>
The required keys are: "{SsoEnvVarParam.CACHE_NAME}", "{SsoEnvVarParam.SECRET_KEY}", "{SsoEnvVarParam.SECRET_SALT_UUID}", "{SsoEnvVarParam.CRYPTO}", "{SsoEnvVarParam.STORAGE}"<br>
Default: None<br>
<br>

## {Constants.MAGIC_CLASS_NAME_UPPER}_DEVICE_CODE_NOTIFICATION_EMAIL
Specifies details to enable email notification. <br>
Should be a list of key='value' pairs separated by a semicolon ';'<br>
The required keys are: "{Email.SMTP_PORT}", "{Email.SMTP_ENDPOINT}", "{Email.SEND_FROM}", "{Email.SEND_TO}", "{Email.SEND_FROM_PASSWORD}"<br>
The optional keys are: "{Email.CONTEXT}"<br>
Default: None<br>
<br>

## {Constants.MAGIC_CLASS_NAME_UPPER}_EXTRAS_REQUIRE
Specifies the same list of extras that were used to install. <br>
if KqlmagicCustom was installed KQLMAGIC_EXTRAS_REQUIRE environment variable should be set to the same list of extras that were used to install (if not set, 'default' will be used)<br>
for example:<br>
KQLMAGIC_EXTRAS_REQUIRE=plotly,pandas,sso<br>
KQLMAGIC_EXTRAS_REQUIRE=default<br>
KQLMAGIC_EXTRAS_REQUIRE=jupyter-basic,widgets<br>
On Kqlmagic load will warn on missing packages that were specified in KQLMAGIC_EXTRAS_REQUIRE (if not set, 'default' will be used)<br>
Features that require a missing dependency will be disabled.<br>
Default: 'default' (all extras)<br>
<br>

## {Constants.MAGIC_CLASS_NAME_UPPER}_WARN_MISSING_DEPENDENCIES
Specify whether to warn on missing dependencies on {Constants.MAGIC_PACKAGE_NAME} load. Valid values are True or False.<br>
Default: True<br>
<br>

## {Constants.MAGIC_CLASS_NAME_UPPER}_NOTEBOOK_SERVICE_ADDRESS
Force the specified backend service address. Only for remote kernels.<br>
Default: None (auto set by jupyter implementation)<br>
<br>

## {Constants.MAGIC_CLASS_NAME_UPPER}_AZUREML_COMPUTE
Specified azureml backend compute host address. If not specified popup windows might not work properly.<br>
Default: value taken from {Constants.MAGIC_CLASS_NAME_UPPER}_NOTEBOOK_SERVICE_ADDRESS<br>
<br>

## {Constants.MAGIC_CLASS_NAME_UPPER}_NOTEBOOK_APP
Force the specified jupyter implementation. valid values are: "auto", "jupyterlab", "azurenotebook", "azureml", "azuremljupyternotebook", "azuremljupyterlab", "jupyternotebook", "ipython", "visualstudiocode", "azuredatastudio", "nteract", "azuredatastudiosaw"<br>
Default: "auto" (auto detect implementation)<br>
<br>

## {Constants.MAGIC_CLASS_NAME_UPPER}_LOAD_MODE
If set to 'silent', banner won't be displayed on {Constants.MAGIC_PACKAGE_NAME} load. valid values: 'silent'<br>
Default: None<br>
<br>

## {Constants.MAGIC_CLASS_NAME_UPPER}_TEMP_FILES_SERVER_PY_FOLDER 
Force default location of 'my_files_server.py' code. (folder path)<br>
Default: None (auto set by jupyter implementation)<br>
<br>

## {Constants.MAGIC_CLASS_NAME_UPPER}_TEMP_FILES_SERVER
Specify implementation of file server. valid values are: "auto", "jupyter", "kqlmagic", "disabled"<br>
Default: "auto" (auto set by jupyter implementation) <br>
<br>

## {Constants.MAGIC_CLASS_NAME_UPPER}_FILES_SERVER_WINDOW_VISIBILITY
Specify whether to show file server process icon. Valid values are "show" or "hide".<br>
Deafult: "hide"<br>
<br>

## {Constants.MAGIC_CLASS_NAME_UPPER}_CONFIGURATION
Override default options. Should be a list of key='value' pairs separated by a semicolon ';'<br>
Default: None<br>
<br>

## {Constants.MAGIC_CLASS_NAME_UPPER}_KERNEL
Specify whether {Constants.MAGIC_CLASS_NAME} kernel should be activated. Valid values are True or False.<br>
Default: False<br>
<br>

## {Constants.MAGIC_CLASS_NAME_UPPER}_CACHE
Specifies a folder name for storing query results that can be used as cache for same queries.<br>
Default:None<br>
<br>

## {Constants.MAGIC_CLASS_NAME_UPPER}_USE_CACHE
Specifies a folder that holds cached query results.<br>
Default: None<br>
<br>
"""


_HELP_SSO = f"""## Overview
- To get data from Azure Monitor data resources, the user need to authenticate itself, and if it has the right permission, 
he would be able to query that data resource.
- It is possible to activate Single Sign On, which will allow a user to go through the authentication process once and remain autheticated for a certain amount of time. 
- To activate Single Sign On:
    1. Set the environmental parameter {Constants.MAGIC_CLASS_NAME_UPPER}_SSO_ENCRYPTION_KEYS with the following parameters:
        - cachename = an identifying name for your SSO cache.
        - secretkey = a password for encryption, for the Single Sign On. Should be at least 8 characters, at least one uppercase Letter, at least 2 digits and at least one non-letter character. Please choose a strong password.
        - secret_salt_uuid = a valid UUID (version 4).
    2. Authenticate normally.
    3. Use this environmental parameter again (with your parameters) to use SSO and authenticate automatically. 
- Additional settings:
    -- Use the option: -enable_sso= False in order to disable SSO- it will not be activated even if correct parameters are given.

{_NEED_SUPPORT_SECTION}"""


_HELP_PROXIES = f"""## Overview
- If you need to use a proxies, configure proxies by setting the environment variables HTTP_PROXY and HTTPS_PROXY.

### http protocol
- Example setting proxies with **http protocol**:<br><br>
```$ export HTTP_PROXY="http://10.10.1.10:3128"```<br><br>
```$ export HTTPS_PROXY="http://10.10.1.10:1080"```<br><br>
<br>
- To use HTTP Basic Auth with your proxy, use the http://user:password@host syntax:<br><br>
```$ export HTTP_PROXY="http://user:pass@10.10.1.10:3128"```<br><br>
```$ export HTTPS_PROXY="http://user:pass@10.10.1.10:1080"```<br><br>
<br>
### socks protocol
- Please make sure to install first requests socks dependency:<br><br>
```$pip install requests[socks]```<br><br>
<br>
- Example setting proxies with **sock protocol**:<br><br>
```$ export HTTP_PROXY="socks5://user:pass@10.10.1.10:3128"```<br><br>
```$ export HTTPS_PROXY="socks5://user:pass@10.10.1.10:1080"```<br><br>
<br>
### Note:
- You can also set the environment variable from within the notebook using %env magic.
"""

_HELP_SAW = f"""## Overview
- Secure Access Workstation (saw) is limiting which python modules are allowed. {Constants.MAGIC_PACKAGE_NAME}Saw is a variant of Kqlmagic package, some of the Kqlmaguc modules are replaced with similar modules that are allowed on 'saw'.<br>
<br>

## Install example
```
pip install {Constants.MAGIC_PACKAGE_NAME}Saw
```
<br>

## Notebook install example
```python
!pip install {Constants.MAGIC_PACKAGE_NAME}Saw
```
<br>

"""

_HELP_SQL = f"""## Overview 
- Azure Data Explorer supports a subset of the SQL language. 
<br>

## Translate SQL to KQL
The primary language to interact with Kusto is KQL (Kusto Query Language). To make the transition and learning experience easier, 
you can use Kusto to translate SQL queries to KQL. Send an SQL query to Kusto, prefixing it with the verb 'EXPLAIN'.
For Example:
```
%%kql
EXPLAIN 
SELECT COUNT_BIG(*) as C FROM StormEvents
```
<br>

## SQL to Kusto cheat sheet
see https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/sqlcheatsheet
<br>

## MS-TDS/T-SQL differences between Azure Data Explorer Microsoft SQL Server
see https://docs.microsoft.com/en-us/azure/data-explorer/kusto/api/tds/sqlknownissues
<br>

"""

_HELP_ARIA = f"""## Overview
you can query Aria underlying ADX databases using KQL via Azure Data Explorer
You have to set the connection string with cluster set to: cluster='https://kusto.aria.microsoft.com'; 
and the database to the Aria ADX database name GUID
cluster='https://kusto.aria.microsoft.com';database='<your-GUID';alias='<your-database-friendly-name>'
<br>

## The All-Seeing Eye of Kusto
see https://www.aria.ms/2019/02/01/monthly-release/
<br>

## Aria + Kusto Explorer
see https://www.aria.ms/2018/02/01/introducing-kusto-explorer/
<br>

## ADX Help
see https://www.aria.ms/search/?q=kusto
<br>

"""


_ADX_CLIENT_REQUEST_PROPERTIES = """## Overview

- The client request properties have many uses. 
Some of them are used to make debugging easier (for example, by providing correlation strings that can be used to track client/service interactions), 
others are used to affect what limits and policies get applied to the request.

##List of Client Request Properties

- **deferpartialqueryfailures**: If true, disables reporting partial query failures as part of the result set. [bool]
- **materialized_view_shuffle**: A hint to use shuffle strategy for materialized views that are referenced in the query. The property is an array of materialized views names and the shuffle keys to use. examples: 'dynamic([ { "Name": "V1", "Keys" : [ "K1", "K2" ] } ])' (shuffle view V1 by K1, K2) or 'dynamic([ { "Name": "V1" } ])' (shuffle view V1 by all keys) [dict]
- **max_memory_consumption_per_query_per_node**: Overrides the default maximum amount of memory a whole query may allocate per node. [int]
- **maxmemoryconsumptionperiterator**: Overrides the default maximum amount of memory a query operator may allocate. [int]
- **maxoutputcolumns**: Overrides the default maximum number of columns a query is allowed to produce. [int]
- **norequesttimeout**: Enables setting the request timeout to its maximum value. [bool]
- **notruncation**: Enables suppressing truncation of the query results returned to the caller. [bool]
- **push_selection_through_aggregation**: If true, push simple selection through aggregation [bool]
- **query_bin_auto_at**: When evaluating the bin_auto() function, the start value to use. [LiteralExpression]
- **query_bin_auto_size**: When evaluating the bin_auto() function, the bin size value to use. [LiteralExpression]
- **query_cursor_after_default**: The default parameter value of the cursor_after() function when called without parameters. [str]
- **query_cursor_before_or_at_default**: The default parameter value of the cursor_before_or_at() function when called without parameters. [str]
- **query_cursor_current**: Overrides the cursor value returned by the cursor_current() or current_cursor() functions. [str]
- **query_cursor_disabled**: Disables usage of cursor functions in the context of the query. [bool]
- **query_cursor_scoped_tables**: List of table names that should be scoped to cursor_after_default .. cursor_before_or_at_default (upper bound is optional). [dynamic]
- **query_datascope**: Controls the query's datascope -- whether the query applies to all data or just part of it. ['default', 'all', or 'hotcache']
- **query_datetimescope_column**: Controls the column name for the query's datetime scope (query_datetimescope_to / query_datetimescope_from). [str]
- **query_datetimescope_from**: Controls the query's datetime scope (earliest) -- used as auto-applied filter on query_datetimescope_column only (if defined). [DateTime]
- **query_datetimescope_to**: Controls the query's datetime scope (latest) -- used as auto-applied filter on query_datetimescope_column only (if defined). [DateTime]
- **query_distribution_nodes_span**: If set, controls the way sub-query merge behaves: the executing node will introduce an additional level in the query hierarchy for each sub-group of nodes; the size of the sub-group is set by this option. [Int]
- **query_fanout_nodes_percent**: The percentage of nodes to fanout execution to. [int]
- **query_fanout_threads_percent**: The percentage of threads to fanout execution to. [int]
- **query_force_row_level_security**: If specified, forces Row Level Security rules, even if row_level_security policy is disabled [bool]
- **query_language**: Controls how the query text is to be interpreted. ['csl','kql' or 'sql']
- **query_max_entities_in_union**: Overrides the default maximum number of columns a query is allowed to produce. [int]
- **query_now**: Overrides the datetime value returned by the now(0s) function. [DateTime]
- **query_python_debug**: If set, generate python debug query for the enumerated python node (default first). [bool/int]
- **query_results_apply_getschema** If set, retrieves the schema of each tabular data in the results of the query instead of the data itself. [bool]
- **query_results_cache_max_age**: If positive, controls the maximum age of the cached query results which Kusto is allowed to return [str]
- **query_results_progressive_row_count**: Hint for Kusto as to how many records to send in each update (Takes effect only in progressive mode) [int]
- **query_results_progressive_update_period**: Hint for Kusto as to how often to send progress frames (Takes effect only if in progressive mode) [int]
- **query_take_max_records**: Enables limiting query results to this number of records. [int]
- **queryconsistency**: Controls query consistency. ['strongconsistency' or 'normalconsistency' or 'weakconsistency']
- **request_block_row_level_security**: If specified, blocks access to tables for which row_level_security policy is enabled [bool]
- **request_callout_disabled**: If specified, indicates that the request cannot call-out to a user-provided service. [bool]
- **request_description**: Arbitrary text that the author of the request wants to include as the request description. [str]
- **request_external_table_disabled**: If specified, indicates that the request cannot invoke code in the ExternalTable. [bool]
- **request_impersonation_disabled**: If specified, indicates that the service shouldn't impersonate the caller's identity. [bool]
- **request_readonly**: If specified, indicates that the request must not be able to write anything. [bool]
- **request_remote_entities_disabled**: If specified, indicates that the request cannot access remote databases and clusters. [bool]
- **request_sandboxed_execution_disabled**: If specified, indicates that the request cannot invoke code in the sandbox. [bool]
- **results_progressive_enabled**: If set, enables the progressive query stream
- **servertimeout**: Overrides the default request timeout. [TimeSpan]
- **truncationmaxrecords**: Overrides the default maximum number of records a query is allowed to return to the caller (truncation). [int]
- **truncationmaxsize**: Overrides the dfefault maximum data size a query is allowed to return to the caller (truncation). [int]
- **validate_permissions**: Validates user's permissions to perform the query and doesn't run the query itself. [bool]
"""

_REQUEST_TAGS = f"""## Overview

- Request tags enables to tag **x-ms-app**, **x-ms-user** and **x-ms-client-request-id** request headers with a custom string.<br>
The main scenario for tagging request headers is to detect tagged query requests within collected queries telemetry repository.<br>
For example within ADX cluster, executing <br>```.show queries```
<br><br>

##List of Request Tags options

 - **request_id_tag** (idtag) option tags **x-ms-client-request-id** header.<br> 
 The tag will be injected as follows: 
        <br>```x-ms-client-request-id: {Constants.MAGIC_CLASS_NAME}.execute;{{tag}};{{guid}}```<br><br>
    - request_id_tag can be set per query request by setting the the option as follows: <br>```%kql -idtag='{{tag}}' {{query}}```<br>
    - request_id_tag can be set for all requests by setting the default tag: <br>```aa {Constants.MAGIC_CLASS_NAME}.request_id_tag='{{tag}}'```<br>
<br>

 - **request_app_tag** (apptag) option tags **x-ms-app** header.<br>
 The tag will be injected as follows: 
        <br>```x-ms-app: {Constants.MAGIC_CLASS_NAME};{{tag}}```<br><br>
    - request_app_tag can be set per query request by setting the the option as follows: <br>```%kql -apptag='{{tag}}' {{query}}```<br>
    - request_app_tag can be set for all requests by setting the default tag: <br>```%config {Constants.MAGIC_CLASS_NAME}.request_app_tag='{{tag}}'```<br>
 <br>

 - **request_user_tag** (usertag) option tags **x-ms-user** header.<br>
 The tag will be injected as follows: 
        <br>```x-ms-user: {{tag}}```<br><br>
    - request_user_tag can be set per query request by setting the the option as follows: <br>```%kql -usertag='{{tag}}' {{query}}```<br>
    - request_user_tag can be set for all requests by setting the default tag: <br>```%config {Constants.MAGIC_CLASS_NAME}.request_user_tag='{{tag}}'```<br>
"""

_HELP = {
    "query": _KQL_URL,
    "kql": _KQL_URL,
    "appinsights": _APPINSIGHTS_URL, 
    "applicationinsights": _APPINSIGHTS_URL,
    "loganalytics": _LOGANALYTICS_URL,
    "azuremonitor": _AZUREMONITOR_URL,
    "kusto": _KUSTO_URL,
    "adx": _KUSTO_URL,
    "azuredataexplorer": _KUSTO_URL,
    "conn": _HELP_CONN,
    "logging": _HELP_LOGGING,
    "help": _HELP_HELP,
    "usage": _USAGE,
    "commands": _HELP_COMMANDS,
    "cache": _HELP_CACHE,
    "faq": _KQLMAGIC_FAQ_URL,
    "config": "config",
    "options": "options",
    "sso": _HELP_SSO,
    "env": _HELP_ENV, 
    "proxies": _HELP_PROXIES,
    "saw": _HELP_SAW,
    "sql": _HELP_SQL,
    "aria": _HELP_ARIA,
    "clientrequestproperties": _ADX_CLIENT_REQUEST_PROPERTIES,
    "requesttags": _REQUEST_TAGS,
    "kqlmagicdownloads": _KQLMAGIC_DOWNLOADS_URL,
    "kqlmagicinstall": _KQLMAGIC_INSTALL_URL,
    "kqlmagicreadme": _KQLMAGIC_README_URL,
    "kqlmagicgithub": _KQLMAGIC_GITHUB_URL,
    "kqlmagiclicense": _KQLMAGIC_LICENSE_URL,
    "kqlmagiccontributors": _KQLMAGIC_CONTRIBUTORS_URL,
}


class UrlReference(object):
    """ A wrapper class that holds a url reference.
    
    Parameters
    ----------
    name : str
        Name of the url.
    url : str
        Reference url.
    button : str
        A string to be presented on a button, that on click will open the url

     """

    def __init__(self, name:str, url:str, button_text:str, is_raw:bool=None):
        self.name = name
        self.url = url 
        self.button_text = button_text
        self.is_raw = is_raw is True

markdown_module = Dependencies.get_module('markdown', dont_throw=True)
bs4_module = Dependencies.get_module('bs4', dont_throw=True)
lxml_module = Dependencies.get_module('lxml', dont_throw=True)

class MarkdownString(object):
    """ A class that holds a markdown string.
    
    can present the string as markdown, html, and text
     """

    global markdown_module
    global bs4_module
    global lxml_module

    def __init__(self, markdown_string:str, title:str=None):
        self.markdown_string = markdown_string
        self.title = f" - {title}" if title else ""


    # Printable unambiguous presentation of the object
    def __repr__(self)->str:
        return self._markdown_to_text(self.markdown_string)


    def _repr_html_(self)->str:
        if markdown_module:
            return self._force_repr_html_()


    def _force_repr_html_(self)->str:
        return f"""
        <html>
        <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width,initial-scale=1">
        <title>{Constants.MAGIC_PACKAGE_NAME}{self.title}</title>
        </head>
        <body>
        {self._markdown_to_html(self.markdown_string)}
        </body>
        </html>
        """


    def _repr_markdown_(self)->str:
        return self.markdown_string
    

    def __str__(self)->str:
        return self.__repr__()


    def _strip_markdown_controls(self, markdown_string:str)->str:
        string = markdown_string.replace("**", "")
        string = string.replace("```", "")
        return string
        

    def _strip_markdown_html_controls(self, markdown_string:str)->str:
        string = markdown_string.replace("<br>", "\n")
        return string


    def _markdown_to_html(self, markdown_string:str)->str:
        if markdown_module:
            html = markdown_module.markdown(markdown_string)
        else:
            string = self._strip_markdown_controls(markdown_string)
            string = string.replace("\n", "<br>")
            html = f"<p>{string}</p>"
        return html

    
    def _markdown_to_text(self, markdown_string:str)->str:
        if markdown_module and bs4_module and lxml_module:
            html = markdown_module.markdown(markdown_string)
            text = ''.join(bs4_module.BeautifulSoup(html, features="lxml").findAll(text=True))
        else:
            text = self._strip_markdown_controls(markdown_string)
            text = self._strip_markdown_html_controls(text)
        return text


def execute_usage_command()->MarkdownString:
    """ execute the usage command.
    command that returns the usage string that will be displayed to the user.

    Returns
    -------
    MarkdownString object
        The usage string wrapped by an object that enable markdown, html or text display.
    """
    return execute_help_command("usage")


def execute_faq_command()->UrlReference:
    """ execute the faq command.
    command that returns the faq string that will be displayed to the user.

    Returns
    -------
    MarkdownString object
        The faq string wrapped by an object that enable markdown, html or text display.
    """
    return execute_help_command("faq")


def execute_help_command(topic:str):
    """ execute the help command.
    command that return the help topic string that will be displayed to the user.

    Returns
    -------
    MarkdownString object
        The help topic string wrapped by an object that enable markdown, html or text display of the topic.
    """
    help_topic_string = _HELP.get(topic.strip().lower().replace("_", "").replace("-", ""))
    if help_topic_string is None:
        raise ValueError(f'"{topic}" unknown help topic')
    elif help_topic_string.startswith("http"):
        button_text = f"popup {topic} reference "
        return UrlReference(topic, help_topic_string, button_text, topic == "faq")
    elif help_topic_string == '':
        help_topic_string = "Sorry, not implemented yet."
    elif help_topic_string in ["config", "options"]:
        return help_topic_string
    return MarkdownString(help_topic_string, title=f"help {topic}")

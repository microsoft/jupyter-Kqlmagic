# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

"""A module that manage package version.
"""

from Kqlmagic.constants import Constants
from bs4 import BeautifulSoup
from markdown import markdown

_KQL_URL = "http://aka.ms/kdocs"
_APPINSIGHTS_URL= "https://docs.microsoft.com/en-us/azure/application-insights/app-insights-overview?toc=/azure/azure-monitor/toc.json"
_LOGANALYTICS_URL = "https://docs.microsoft.com/en-us/azure/log-analytics/log-analytics-queries?toc=/azure/azure-monitor/toc.json"
_AZUREMONITOR_URL = "https://docs.microsoft.com/en-us/azure/azure-monitor/"
_KUSTO_URL = "https://docs.microsoft.com/en-us/azure/data-explorer/"

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

- **version** - Displays the current version string.<br>
<br>

- **usage** - Displays usage of Kqlmagic.<br>
<br>

- **help "topic"** - Display information about the topic.<br>
    - To get the list of all the topics, execute ```%kql --help "help"```<br>
<br>

## Examples:
```%kql --version```<br><br>
```%kql --usage```<br><br>
```%kql --help "help"```<br><br>
```%kql --help "options"```<br><br>
```%kql --help "conn"```<br><br>
```%kql --submit appinsights://appid='DEMO_APP';appkey='DEMO_KEY' pageViews | count```<br><br>
```%kql pageViews | count```
"""
_FAQ = """
"""
_USAGE = """## Usage:
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

""" +_NEED_SUPPORT_SECTION

_HELP_HELP = """## Overview
Help command is a tool to get more information on a topics that are relevant to Kqlmagic.
t
usage: ```%kql --help "topic"```<br>

## Topics
- **usage** - How to use the Kqlmagic.<br>
<br>

- **conn** - Lists the available connection string variation, and how their are used to authenticatie to data sources.<br>
<br>

- **query** / **kql** - [Reference to resources Kusto Queru language, aka kql, documentation](""" +_KQL_URL+ """)<br>
<br>

- **options** - Lists the available options, and their behavior impact on the submit query command.<br>
<br>

- **commands** - Lists the available commands, and what they do.<br>
<br>

- **faq** - Lists frequently asked quetions and answers.<br>
<br>

- **help** - This help.<br>
<br>

- **AzureMonitor**- [Reference to resources Azure Monitor tools](""" +_AZUREMONITOR_URL+ """)<br>
Azure Monitor, which now includes Log Analytics and Application Insights, provides sophisticated tools for collecting and analyzing telemetry that allow you to maximize the performance and availability of your cloud and on-premises resources and applications. It helps you understand how your applications are performing and proactively identifies issues affecting them and the resources they depend on.
<br>

- **AzureDataExplorer** / **kusto**- [Reference to resources Azure Data Explorer (kusto) service](""" +_KUSTO_URL+ """)<br>
Azure Data Explorer is a fast and highly scalable data exploration service for log and telemetry data. It helps you handle the many data streams emitted by modern software, so you can collect, store, and analyze data. Azure Data Explorer is ideal for analyzing large volumes of diverse data from any data source, such as websites, applications, IoT devices, and more.
<br>

- **LogAnalytics**- [Reference to resources Log Analytics service](""" +_LOGANALYTICS_URL+ """)<br>
Log data collected by Azure Monitor is stored in Log Analytics which collects telemetry and other data from a variety of sources and provides a query language for advanced analytics.
<br>

- **ApplicationInsights** / **AppInsights**- [Reference to resources Application Insights service](""" +_APPINSIGHTS_URL+ """)<br>
Application Insights is an extensible Application Performance Management (APM) service for web developers on multiple platforms. Use it to monitor your live web application. It will automatically detect performance anomalies. It includes powerful analytics tools to help you diagnose issues and to understand what users actually do with your app. It's designed to help you continuously improve performance and usability. It works for apps on a wide variety of platforms including .NET, Node.js and J2EE, hosted on-premises or in the cloud. It integrates with your DevOps process, and has connection points to a variety of development tools. It can monitor and analyze telemetry from mobile apps by integrating with Visual Studio App Center.
<br>


""" +_NEED_SUPPORT_SECTION

_HELP_CONN = """## Overview
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

## Connect to Azure Data Explorere (kusto) data resource ```<database or alias>@<cluster>```
Few options to authenticate with Azure Data Explorer (Kusto) data resources:<br>
```%kql kusto://code;cluster='<cluster-name>';database='<database-name>';alias='<database-friendly-name>'```<br><br>
```%kql kusto://tenant='<tenant-id>';clientid='<aad-appid>';clientsecret='<aad-appkey>';cluster='<cluster-name>';database='<database-name>';alias='<database-friendly-name>'```<br><br>
```%kql kusto://tenant='<tenant-id>';certificate='<certificate>';certificate_thumbprint='<thumbprint>';cluster='<cluster-name>';database='<database-name>';alias='<database-friendly-name>'```<br><br>
```%kql kusto://tenant='<tenant-id>';certificate_pem_file='<pem_filename>';certificate_thumbprint='<thumbprint>';cluster='<cluster-name>';database='<database-name>';alias='<database-friendly-name>'```<br><br>
```%kql kusto://username='<username>';password='<password>';cluster='<cluster-name>';database='<database-name>';alias='<database-friendly-name>'```<br><br>

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

Notes:<br>
- username/password works only on corporate network.<br>
- alias is optional.<br>
- if credentials are missing, and a previous connection was established the credentials will be inherited.<br>
- if secret (password / clientsecret / appkey) is missing, user will be prompted to provide it.<br>
- if tenant is missing, and a previous connection was established the tenant will be inherited.<br>
- **a not quoted value, is a python expression, that is evaluated and its result is used as the value. This is how you can parametrize the connection string**


""" +_NEED_SUPPORT_SECTION

_HELP = {
    "query" : _KQL_URL,
    "kql": _KQL_URL,
    "appinsights": _APPINSIGHTS_URL, 
    "applicationinsights": _APPINSIGHTS_URL,
    "loganalytics": _LOGANALYTICS_URL,
    "azuremonitor": _AZUREMONITOR_URL,
    "kusto": _KUSTO_URL,
    "azuredataexplorer": _KUSTO_URL,
    "conn" : _HELP_CONN,
    "options" : "",
    "help" : _HELP_HELP,
    "usage" : _USAGE,
    "commands" : _HELP_COMMANDS,
    "cache" : "",
    "faq" : "",
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
    def __init__(self, name: str, url: str, button_text: str):
        self.name = name
        self.url = url 
        self.button_text = button_text

class MarkdownString(object):
    """ A class that holds a markdown string.
    
    can present the string as markdown, html, and text
     """

    def __init__(self, markdown_string: str):
        self.markdown_string = markdown_string

    # Printable unambiguous presentation of the object
    def __repr__(self):
        html = self._repr_html_()
        return ''.join(BeautifulSoup(html, features="lxml").findAll(text=True))

    def _repr_html_(self):
        return markdown(self.markdown_string)

    def _repr_markdown_(self):
       return self.markdown_string
    
    def __str__(self):
        return self.__repr__()

def execute_usage_command() -> MarkdownString:
    """ execute the usage command.
    command that returns the usage string that will be displayed to the user.

    Returns
    -------
    MarkdownString object
        The usage string wrapped by an object that enable markdown, html or text display.
    """
    return execute_help_command("usage")

def execute_faq_command() -> MarkdownString:
    """ execute the faq command.
    command that returns the faq string that will be displayed to the user.

    Returns
    -------
    MarkdownString object
        The faq string wrapped by an object that enable markdown, html or text display.
    """
    return execute_help_command("faq")

def execute_help_command(topic: str) -> MarkdownString:
    """ execute the help command.
    command that return the help topic string that will be displayed to the user.

    Returns
    -------
    MarkdownString object
        The help topic string wrapped by an object that enable markdown, html or text display of the topic.
    """
    help_topic_string = _HELP.get(topic.strip().lower().replace("_", "").replace("-", ""))
    if help_topic_string is None:
        raise ValueError("{0} unknown help topic".format(topic))
    if help_topic_string.startswith("http"):
        button_text = "popup {0} reference ".format(topic)
        return UrlReference(topic, help_topic_string, button_text)
    elif help_topic_string == '':
        help_topic_string = "Sorry, not implemented yet."
    return MarkdownString(help_topic_string)

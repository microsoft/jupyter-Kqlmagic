#-------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
#--------------------------------------------------------------------------

import os
from kql.parser import Parser
from six.moves import configparser
try:
    from traitlets.config.configurable import Configurable
except ImportError:
    from IPython.config.configurable import Configurable

empty_config = Configurable()
default_options = {'result_var': None}
def test_parse_no_kql():
    assert Parser.parse("dbname@clustername", empty_config) == \
           {'connection': "dbname@clustername",
            'kql': '',
            'options': default_options}

query1 = "let T = view () { datatable(n:long, name:string)[1,'foo',2,'bar'] }; T"
def test_parse_with_kql():
    assert Parser.parse("dbname@clustername {}".format(query1),
                 empty_config) == \
           {'connection': "dbname@clustername",
            'kql': query1,
            'options': default_options}

def test_parse_kql_only():
    parsed = Parser.parse(query1, empty_config)
    print(parsed)
    assert parsed == \
           {'connection': "",
            'kql': query1,
            'options': default_options}

def test_parse_kusto_socket_connection():
    assert Parser.parse("kusto://cluster('clustername').database('dbname') {}".format(query1), empty_config) == \
           {'connection': "kusto://cluster('clustername').database('dbname')",
            'kql': query1,
            'options': default_options}

def test_expand_environment_variables_in_connection():
    os.environ['KQL_DATABASE'] = "kusto://cluster('clustername').database('dbname')"
    assert Parser.parse("$KQL_DATABASE {}".format(query1), empty_config) == \
            {'connection': "kusto://cluster('clustername').database('dbname')",
            'kql': query1,
            'options': default_options}

def test_parse_kusto_socket_connection_with_credentials():
    assert Parser.parse("kusto://username('username').password('password').cluster('clustername').database('dbname') {}".format(query1), empty_config) == \
           {'connection': "kusto://username('username').password('password').cluster('clustername').database('dbname')",
            'kql': query1,
            'options': default_options}

def test_parse_kusto_socket_connection_with_env_credentials():
    os.environ['USERNAME'] = "'michael'"
    os.environ['PASSWORD'] = "'michael123'"
    assert Parser.parse("kusto://username($USERNAME).password($PASSWORD).cluster('clustername').database('dbname') {}".format(query1), empty_config) == \
           {'connection': "kusto://username('michael').password('michael123').cluster('clustername').database('dbname')",
            'kql': query1,
            'options': default_options}

def test_parse_kusto_socket_connection_dsn():
    assert Parser.parse("kusto://username($USERNAME).password($PASSWORD).cluster('clustername').database('dbname') {}".format(query1), empty_config) == \
            {'connection': "kusto://username('michael').password('michael123').cluster('clustername').database('dbname')",
            'kql': query1,
            'options': default_options}

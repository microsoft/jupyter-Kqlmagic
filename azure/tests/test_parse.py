#-------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
#--------------------------------------------------------------------------

import os
from Kqlmagic.parser import Parser
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
    conn_str = "{}://cluster('clustername').database('dbname')".format(TEST_URI_SCHEMA_NAME)
    assert Parser.parse("{0} {1}".format(conn_str, query1), empty_config) == \
           {'connection': conn_str,
            'kql': query1,
            'options': default_options}

def test_expand_environment_variables_in_connection():
    conn_str = "{}://cluster('clustername').database('dbname')".format(TEST_URI_SCHEMA_NAME)
    os.environ['KQL_DATABASE'] = conn_str
    assert Parser.parse("$KQL_DATABASE {}".format(query1), empty_config) == \
            {'connection': conn_str,
            'kql': query1,
            'options': default_options}

def test_parse_kusto_socket_connection_with_credentials():
    conn_str = "{}://username('username').password('password').cluster('clustername').database('dbname')".format(TEST_URI_SCHEMA_NAME)
    assert Parser.parse("{0} {1}".format(conn_str, query1), empty_config) == \
           {'connection': conn_str,
            'kql': query1,
            'options': default_options}

def test_parse_kusto_socket_connection_with_env_credentials():
    conn_str = "{}://username($USERNAME).password($PASSWORD).cluster('clustername').database('dbname')".format(TEST_URI_SCHEMA_NAME)
    result_conn_str = "{}://username('michael').password('michael123').cluster('clustername').database('dbname')".format(TEST_URI_SCHEMA_NAME)
    os.environ['USERNAME'] = "'michael'"
    os.environ['PASSWORD'] = "'michael123'"
    assert Parser.parse("{0} {1}".format(conn_str, query1), empty_config) == \
           {'connection': result_conn_str,
            'kql': query1,
            'options': default_options}

def test_parse_kusto_socket_connection_dsn():
    conn_str = "{}://username($USERNAME).password($PASSWORD).cluster('clustername').database('dbname')".format(TEST_URI_SCHEMA_NAME)
    result_conn_str = "{}://username('michael').password('michael123').cluster('clustername').database('dbname')".format(TEST_URI_SCHEMA_NAME)
    assert Parser.parse("{0} {1}".format(conn_str, query1), empty_config) == \
            {'connection': result_conn_str,
            'kql': query1,
            'options': default_options}

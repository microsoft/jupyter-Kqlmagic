#-------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
#--------------------------------------------------------------------------

from kql.display import Display
from kql.kusto_engine import KustoEngine
from kql.ai_engine import AppinsightsEngine
from kql.la_engine import LoganalyticsEngine
from kql.cache_engine import CacheEngine
from kql.cache_client import CacheClient
from kql.help_html import Help_html


class Database_html(object):
    """
    """
    database_metadata_css = """.just-padding {
      height: 100%;
      width: 100%;
      padding: 15px;
    }

    .list-group.list-group-root {
      padding: 0;
      overflow: hidden;
    }

    .list-group.list-group-root .list-group {
      margin-bottom: 0;
    }

    .list-group.list-group-root .list-group-item {
      border-radius: 0;
      border-width: 1px 0 0 0;
    }

    .list-group.list-group-root > .list-group-item:first-child {
      border-top-width: 0;
    }

    .list-group.list-group-root > .list-group > .list-group-item {
      padding-left: 60px;
    }

    .list-group.list-group-root > .list-group > .list-group > .list-group-item {
      padding-left: 60px;
    }"""

    database_metadata_scripts = """
        <link href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.5/css/bootstrap.min.css" rel="stylesheet" type="text/css">
        <script src="https://ajax.googleapis.com/ajax/libs/jquery/1.11.3/jquery.min.js" type="text/javascript"></script>
        <script src="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.5/js/bootstrap.min.js" type="text/javascript"></script>
                <script type="text/javascript">
                    var w = screen.width;
                    var h = screen.height;            
                    window.resizeTo(w/4, h);
                    window.focus(); 
                </script>
        <script type="text/javascript">
            window.onload=function(){
      
        $(function() {

          $('.list-group-item').on('click', function() {
            $('.glyphicon', this)
              .toggleClass('glyphicon-chevron-right')
              .toggleClass('glyphicon-chevron-down');
          });

        });

            }
        </script>
    """
    database_metadata_html = """<html><head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width,initial-scale=1">
        <title>{0}</title>

        </head><body>
        {1}
        <style>
        {2}
        </style>

        <h1 align="center">{3}</h1>

        <div class="just-padding">

        <div class="list-group list-group-root well">
        {4}
        </div></div></body></html>"""

    @staticmethod
    def convert_database_metadata_to_html(database_metadata_tree, connectionName, **kwargs):
        item = ""
        for table in database_metadata_tree.keys():
            table_metadata_tree = database_metadata_tree.get(table)
            item += Database_html._convert_table_metadata_tree_to_item(table, table_metadata_tree, **kwargs)
        header = connectionName
        title = connectionName.replace("@", "_at_") + " schema"
        result = Database_html.database_metadata_html.format(
            title, Database_html.database_metadata_scripts, Database_html.database_metadata_css, header, item
        )
        # print(result)
        return result

    @staticmethod
    def _create_database_metadata_tree(rows, databaseName, **kwargs):
        database_metadata_tree = {}
        for row in rows:
            database_name = row["DatabaseName"]
            table_name = row["TableName"]
            column_name = row["ColumnName"]
            column_type = row["ColumnType"]
            if database_name == databaseName:
                if table_name and len(table_name) > 0:
                    if not database_metadata_tree.get(table_name):
                        database_metadata_tree[table_name] = {}
                    if column_name and len(column_name) > 0 and column_type and len(column_type) > 0:
                        database_metadata_tree.get(table_name)[column_name] = column_type
        return database_metadata_tree

    @staticmethod
    def _create_database_draft_metadata_tree(rows, **kwargs):
        database_metadata_tree = {}
        for row in rows:
            table_name = row["name"]
            if table_name and len(table_name) > 0:
                database_metadata_tree[table_name] = {}
                for col in row['columns']:
                    column_name = col["name"]
                    column_type = col["type"]
                    if column_name and len(column_name) > 0 and column_type and len(column_type) > 0:
                        database_metadata_tree.get(table_name)[column_name] = column_type
        return database_metadata_tree

    @staticmethod
    def _convert_table_metadata_tree_to_item(table, table_metadata_tree, **kwargs):
        item = (
            """<a href='#"""
            + table
            + """' class="list-group-item" data-toggle="collapse">
                     <i class="glyphicon glyphicon-chevron-right"></i><b>"""
            + table
            + """</b>
                  </a>
                  <div class="list-group collapse" id='"""
            + table
            + """'>"""
        )
        for column_name in table_metadata_tree.keys():
            column_type = table_metadata_tree.get(column_name)
            if column_type.startswith("System."):
                column_type = column_type[7:]
            item += Database_html._convert_column_metadata_to_item(column_name, column_type, **kwargs)
        item += """</div>"""
        return item

    @staticmethod
    def _convert_column_metadata_to_item(column_name, column_type, **kwargs):
        col_metadata = {}
        item = "<b>" + column_name + "</b> : " + column_type
        return """<a href="#" class="list-group-item">""" + item + """</a>"""

    @staticmethod
    def get_schema_file_path(conn, **kwargs):
        engine_type = (KustoEngine if isinstance(conn, KustoEngine) or (isinstance(conn, CacheEngine) and isinstance(conn.kql_engine, KustoEngine))
                       else AppinsightsEngine if isinstance(conn, AppinsightsEngine) or (isinstance(conn, CacheEngine) and isinstance(conn.kql_engine, AppinsightsEngine))
                       else LoganalyticsEngine if isinstance(conn, LoganalyticsEngine) or (isinstance(conn, CacheEngine) and isinstance(conn.kql_engine, LoganalyticsEngine))
                       else None)

        if engine_type is not None:
            if isinstance(conn, CacheEngine):
                database_name = conn.kql_engine.get_database()
                conn_name = conn.kql_engine.get_conn_name()
            else:
                database_name = conn.get_database()
                conn_name = conn.get_conn_name()


            if engine_type == KustoEngine:
                query = ".show schema"
                raw_query_result = conn.execute(query)
                raw_schema_table = raw_query_result.tables[0]
                database_metadata_tree = Database_html._create_database_metadata_tree(raw_schema_table.fetchall(), database_name)
                if kwargs.get('cache') and not kwargs.get('use_cache') and not isinstance(conn, CacheEngine):
                    CacheClient().save(raw_query_result, conn.get_database(), conn.get_cluster(), query, **kwargs)

            elif engine_type == AppinsightsEngine or LoganalyticsEngine:
                query = ".show schema"
                metadata_result = conn.client_execute(query)
                metadata_schema_table = metadata_result.table
                database_metadata_tree = Database_html._create_database_draft_metadata_tree(metadata_schema_table)
                if kwargs.get('cache') and not kwargs.get('use_cache') and not isinstance(conn, CacheEngine):
                    CacheClient().save(metadata_result, conn.get_database(), conn.get_cluster(), query, **kwargs)

            html_str = Database_html.convert_database_metadata_to_html(database_metadata_tree, conn_name)
            window_name = "_" + conn_name.replace("@", "_at_") + "_schema"
            return Display._html_to_file_path(html_str, window_name, **kwargs)
        else:
            return None

    @staticmethod
    def popup_schema(file_path, conn):
        if file_path:
            conn_name = conn.kql_engine.get_conn_name() if isinstance(conn, CacheEngine) else conn.get_conn_name()
            button_text = "popup schema " + conn_name
            window_name = "_" + conn_name.replace("@", "_at_") + "_schema"
            Display.show_window(window_name, file_path, button_text=button_text, onclick_visibility="visible")

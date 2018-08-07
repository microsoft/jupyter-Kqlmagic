from kql.display  import Display
from kql.kusto_engine import KustoEngine
from kql.ai_engine import AppinsightsEngine
from kql.help_html import Help_html


class Database_html(object):
    """
    """

    # optional queries to get list, itemType is as the table name without the s
    # union * | summarize any(*) by itemType | project itemType
    # %kql union * | project itemType | summarize any(*) by itemType
    # %kql union * | project itemType | distinct *
    application_insights_tables = ['requests', 'exceptions', 'dependencies', 'traces', 'events', 'customEvents', 'metrics', 'customMetrics', 'pageViews', 'browserTimings', 
                                   'availabilityResults']
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
        item = ''
        for table in database_metadata_tree.keys():
            table_metadata_tree = database_metadata_tree.get(table)
            item += Database_html._convert_table_metadata_tree_to_item(table, table_metadata_tree, **kwargs)
        header = connectionName
        title = connectionName.replace('@', '_at_') + ' schema'
        result =  Database_html.database_metadata_html.format(title, Database_html.database_metadata_scripts, Database_html.database_metadata_css,
                                                              header, item)
        #print(result)
        return result

    @staticmethod
    def _create_database_metadata_tree(rows, databaseName, **kwargs):
        database_metadata_tree = {}
        for row in rows:
            database_name = row['DatabaseName']
            table_name = row['TableName']
            column_name = row['ColumnName']
            column_type = row['ColumnType']
            if database_name == databaseName:
                if table_name and len(table_name) > 0:
                   if not database_metadata_tree.get(table_name):
                       database_metadata_tree[table_name] = {}
                   if column_name and len(column_name) > 0 and column_type and len(column_type) > 0:
                       database_metadata_tree.get(table_name)[column_name] = column_type
        return database_metadata_tree

    @staticmethod
    def _convert_table_metadata_tree_to_item(table, table_metadata_tree, **kwargs):
        item = """<a href='#""" +table+ """' class="list-group-item" data-toggle="collapse">
                     <i class="glyphicon glyphicon-chevron-right"></i><b>""" +table+ """</b>
                  </a>
                  <div class="list-group collapse" id='""" +table+ """'>"""
        for column_name in table_metadata_tree.keys():
            column_type = table_metadata_tree.get(column_name)
            if column_type.startswith('System.'):
                column_type = column_type[7:]
            item += Database_html._convert_column_metadata_to_item(column_name, column_type, **kwargs)
        item += """</div>"""
        return item

    @staticmethod
    def _convert_column_metadata_to_item(column_name, column_type, **kwargs):
        col_metadata = {}
        item = '<b>' +column_name+ '</b> : ' + column_type
        return """<a href="#" class="list-group-item">""" +item+ """</a>"""

    @staticmethod
    def popup_schema(conn):
        if isinstance(conn, KustoEngine) or isinstance(conn, AppinsightsEngine):
            database_name = conn.get_database()
            conn_name = conn.get_name()

            if isinstance(conn, KustoEngine):
                query = '.show schema'
                raw_table = conn.execute(query)
                database_metadata_tree = Database_html._create_database_metadata_tree(raw_table.fetchall(), database_name)

            elif isinstance(conn, AppinsightsEngine):
                database_metadata_tree = {}
                for table_name in Database_html.application_insights_tables:
                    query = table_name + " | getschema"
                    try:
                        raw_table = conn.execute(query)
                        rows = raw_table.fetchall()
                        if (raw_table.returns_rows()):
                            database_metadata_tree[table_name] = {}
                            for row in rows:
                                column_name = row['ColumnName']
                                column_type = row['ColumnType']
                                if column_name and len(column_name) > 0 and column_type and len(column_type) > 0:
                                    database_metadata_tree.get(table_name)[column_name] = column_type
                    except:
                        pass
            html_str = Database_html.convert_database_metadata_to_html(database_metadata_tree, conn_name)
            window_name = conn_name.replace('@','_at_') + '_schema'
            url = Display._html_to_url(html_str, window_name)
            botton_text = 'popup schema ' + conn_name
            Help_html.add_menu_item(conn_name, url)
            Display.show_window(window_name, url, botton_text)
        else:
            return None

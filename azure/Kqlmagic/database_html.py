# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

from typing import Any, List, Dict
import re


from .constants import Constants
from .display import Display
from .kql_engine import KqlEngine
from .kusto_engine import KustoEngine
from .aria_engine import AriaEngine
from .ai_engine import AppinsightsEngine
from .aimon_engine import AimonEngine
from .la_engine import LoganalyticsEngine
from .cache_engine import CacheEngine
from .cache_client import CacheClient


class Database_html(object):
    """
    """

    _database_metadata_css = """.just-padding {
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

    _database_metadata_scripts = """
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

    _database_metadata_html = """<html><head>
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


    @classmethod
    def _convert_database_metadata_to_html(cls, database_metadata_tree:Dict[str,Any], conn_name:str, **kwargs)->str:
        item = ""
        for table_name in database_metadata_tree.keys():
            table_metadata_tree = database_metadata_tree.get(table_name)
            item += cls._convert_table_metadata_tree_to_item(table_name, table_metadata_tree, **kwargs)
        header = conn_name
        title = f"{Constants.MAGIC_PACKAGE_NAME} - {conn_name.replace('@', '_at_')} schema"
        result = cls._database_metadata_html.format(
            title, cls._database_metadata_scripts, cls._database_metadata_css, header, item
        )
        return result


    @classmethod
    def _create_database_metadata_tree(cls, rows, database_name:str, **kwargs)->Dict[str,Any]:
        database_metadata_tree = {}
        for row in rows:
            database_name:str = row["DatabaseName"]
            table_name = row["TableName"]
            column_name = row["ColumnName"]
            column_type = row["ColumnType"]
            if database_name.lower() == database_name.lower():
                if table_name and len(table_name) > 0:
                    if not database_metadata_tree.get(table_name):
                        database_metadata_tree[table_name] = {}
                    if column_name and len(column_name) > 0 and column_type and len(column_type) > 0:
                        database_metadata_tree.get(table_name)[column_name] = column_type
        return database_metadata_tree


    @classmethod
    def _create_database_draft_metadata_tree(cls, rows:List[Dict[str,Any]], **kwargs)->Dict[str,Any]:
        database_metadata_tree = {}
        for row in rows:
            table_name = row["name"]
            if table_name and len(table_name) > 0:
                database_metadata_tree[table_name] = {}
                for col in row["columns"]:
                    column_name = col["name"]
                    column_type = col["type"]
                    if column_name and len(column_name) > 0 and column_type and len(column_type) > 0:
                        database_metadata_tree.get(table_name)[column_name] = column_type
        return database_metadata_tree


    @classmethod
    def _convert_table_metadata_tree_to_item(cls, table_name:str, table_metadata_tree:Dict[str,Any], **kwargs):
        metadata_items = []
        for column_name in table_metadata_tree.keys():
            column_type = table_metadata_tree.get(column_name)
            if column_type.startswith("System."):
                column_type = column_type[7:]
            metadata_item = cls._convert_column_metadata_to_item(column_name, column_type, **kwargs)
            metadata_items.append(metadata_item)
            
        item = (
            f"""<a href='#{table_name}' class="list-group-item" data-toggle="collapse">
                     <i class="glyphicon glyphicon-chevron-right"></i><b>{table_name}</b></a>
                  <div class="list-group collapse" id='{table_name}'>{''.join(metadata_items)}</div>"""
        )
        return item


    @classmethod
    def _convert_column_metadata_to_item(cls, column_name:str, column_type:str, **kwargs)->str:
        item = f"<b>{column_name}</b> : {column_type}"
        return f"""<a href="#" class="list-group-item">{item}</a>"""


    @classmethod
    def get_schema_tree(cls, engine:KqlEngine, **options)->Dict[str,Any]:
        engine_class = (
            AriaEngine
            if isinstance(engine, AriaEngine) or (isinstance(engine, CacheEngine) and isinstance(engine.kql_engine, AriaEngine))
            # must be after AriaEngine, because AriaEngine class inherit from KustoEngine
            else KustoEngine
            if isinstance(engine, KustoEngine) or (isinstance(engine, CacheEngine) and isinstance(engine.kql_engine, KustoEngine))
            else LoganalyticsEngine
            if isinstance(engine, LoganalyticsEngine) or (isinstance(engine, CacheEngine) and isinstance(engine.kql_engine, LoganalyticsEngine))
            else AimonEngine
            if isinstance(engine, AimonEngine) or (isinstance(engine, CacheEngine) and isinstance(engine.kql_engine, AimonEngine))
            # must be after AimonEngine, because AimonEngine class inherit from AppinsightsEngine
            else AppinsightsEngine 
            if isinstance(engine, AppinsightsEngine) or (isinstance(engine, CacheEngine) and isinstance(engine.kql_engine, AppinsightsEngine))
            else None
        )

        if engine_class is not None:
            if isinstance(engine, CacheEngine):
                client_database_name = engine.kql_engine.get_client_database_name()
            else:
                client_database_name = engine.get_client_database_name()

            if engine_class in [KustoEngine, AriaEngine]:
                show_schema_query = f".show database ['{cls._adjustToKustoEntityNameRules(client_database_name)}'] schema"
                raw_query_result = engine.execute(show_schema_query, **options)
                raw_schema_table = raw_query_result.tables[0]
                database_metadata_tree = cls._create_database_metadata_tree(raw_schema_table.fetchall(), client_database_name)
                if options.get("cache") is not None and options.get("cache") != options.get("use_cache"):
                    CacheClient(**options).save(raw_query_result, engine, show_schema_query, **options)
                return database_metadata_tree

            elif engine_class in [AppinsightsEngine, LoganalyticsEngine, AimonEngine]:
                show_schema_query = ".show schema"
                metadata_result = engine.client_execute(show_schema_query, **options)
                metadata_schema_table = metadata_result.table
                database_metadata_tree = cls._create_database_draft_metadata_tree(metadata_schema_table)
                if options.get("cache") is not None and options.get("cache") != options.get("use_cache"):
                    CacheClient(**options).save(metadata_result, engine, show_schema_query, **options)
                return database_metadata_tree
        return None


    @classmethod
    def _adjustToKustoEntityNameRules(cls, name:str)->str:
        if isinstance(name, str):
            name = re.sub(r'[\s\n\r\f\t]+', ' ', name.strip())
            name = re.sub(r'[^0-9a-zA-Z._\s-]+', ' ', name)
        return name


    @classmethod
    def get_schema_file_path(cls, engine:KqlEngine, **options)->None:
        database_metadata_tree = cls.get_schema_tree(engine, **options)
        if database_metadata_tree is not None:
            if isinstance(engine, CacheEngine):
                conn_name = engine.kql_engine.get_conn_name()
            else:
                conn_name = engine.get_conn_name()
            html_str = cls._convert_database_metadata_to_html(database_metadata_tree, conn_name)
            window_name = f"_{conn_name.replace('@', '_at_')}_schema"
            return Display._html_to_file_path(html_str, window_name, **options)
        else:
            return None


    @classmethod
    def popup_schema(cls, file_path:str, engine:KqlEngine, **options)->None:
        if file_path:
            conn_name = engine.kql_engine.get_conn_name() if isinstance(engine, CacheEngine) else engine.get_conn_name()
            button_text = f"popup schema {conn_name}"
            window_name = f"_{conn_name.replace('@', '_at_')}_schema"
            Display.show_window(window_name, file_path, button_text=button_text, onclick_visibility="visible", content="schema", **options)

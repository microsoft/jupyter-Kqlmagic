# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

from typing import Type, List, Dict


from .engine import Engine 
from .kusto_engine import KustoEngine
from .aria_engine import AriaEngine
from .ai_engine import AppinsightsEngine
from .aimon_engine import AimonEngine
from .la_engine import LoganalyticsEngine
from .cache_engine import CacheEngine
from .constants import ConnStrKeys, Schema
from .exceptions import KqlEngineError


class ConnectionError(Exception):
    pass


class Connection(object):

    _current_engine:Engine = None
    _engine_by_id:Dict[str,Engine] = {}
    _id_by_name:Dict[str,str] = {}
    _last_current_by_engine_class:Dict[Type[Engine], Engine] = {}

    _ENGINE_CLASS_LIST:Type[Engine] = [KustoEngine, AriaEngine, AppinsightsEngine, AimonEngine, LoganalyticsEngine, CacheEngine]
    _ENGINE_CLASS_BY_SCHEMA:Dict[str,Type[Engine]] = {}
    _RESERVED_CLUSTER_NAMES:List[str] = []
    
    # n = e = None
    engine_class:Type[Engine] = None
    for engine_class in _ENGINE_CLASS_LIST:
        for schema_name in engine_class.get_alt_uri_schema_names():
            _ENGINE_CLASS_BY_SCHEMA[schema_name] = engine_class
        reserved = engine_class.get_reserved_cluster_name()
        if reserved is not None:
            _RESERVED_CLUSTER_NAMES.append(reserved)


    @classmethod
    def is_empty(cls)->bool:
        return len(cls._engine_by_id) == 0


    @classmethod
    def _create_engine(cls, conn_str:str, user_ns:dict, **options)->Engine:
        engine_class = cls._find_engine_class_by_conn_str(conn_str)
        # wasn't found in connection list, but maybe a kusto database connection
        if engine_class is None:
            if "@" in conn_str:
                engine_class = KustoEngine
            else:
                valid_prefixes_str = ", ".join([f"{s}://" for s in cls._ENGINE_CLASS_BY_SCHEMA.keys()])
                raise KqlEngineError(f"invalid connection_str, unknown <uri schema name>. valid uri schemas are: {valid_prefixes_str}")

        last_current_engine:Engine = cls._last_current_by_engine_class.get(engine_class.__name__)

        if engine_class not in [KustoEngine, AriaEngine]:
            engine = engine_class(conn_str, user_ns, current=last_current_engine, **options)

        else:
            if "://" in conn_str:
                if last_current_engine is not None:
                    last_cluster_friendly_name = last_current_engine.get_cluster_friendly_name()
                    last_current_engine = cls._engine_by_id.get(f"@{last_cluster_friendly_name}")
                cluster_engine = engine_class(conn_str, user_ns, current=last_current_engine, conn_class=Connection, **options)

                cluster_friendly_name = cluster_engine.get_cluster_friendly_name()

                cls._save_and_set_current_engine(cluster_engine, name=f"@{cluster_friendly_name}")
                database_name = cluster_engine.get_database_name()
                alias = cluster_engine.get_alias()

            elif "@" in conn_str:
                database_name, cluster_friendly_name = conn_str.split("@", 1)
                alias = None
                if len(database_name) < 1:
                    raise KqlEngineError(f"invalid connection_str, key {ConnStrKeys.DATABASE} cannot be empty")
            else:
                raise KqlEngineError(f"invalid connection_str: {conn_str}")
            engine = cls._create_kusto_database_engine(engine_class, database_name, cluster_friendly_name, alias, user_ns, **options)

        if options.get("use_cache") and engine_class != CacheEngine:
            engine = CacheEngine(engine, user_ns, last_current_engine, **options)
        return engine


    @classmethod
    def _find_engine_class_by_conn_str(cls, conn_str:str)->Type[Engine]:
        if conn_str is not None:
            parts = conn_str.split("://", 1)
            if len(parts) == 2:
                uri_schema = parts[0].lower().replace("_", "").replace("-", "")
                return cls._ENGINE_CLASS_BY_SCHEMA.get(uri_schema)


    @classmethod
    def _create_kusto_database_engine(cls, engine_class:Engine,  database_name:str, cluster_friendly_name:str, alias:str, user_ns:dict, **options)->KustoEngine:
        if engine_class._RESERVED_CLUSTER_NAME != cluster_friendly_name and cluster_friendly_name in cls._RESERVED_CLUSTER_NAMES:
            raise KqlEngineError(
                f'invalid connection_str, connection_str pattern "{(alias or database_name)}@{cluster_friendly_name}" '
                f'cannot be used for reserved "appinsights", "loganalytics" and "cache"'
            )
        cluster_engine = cls.get_engine_by_name(f"@{cluster_friendly_name}")
        if cluster_engine is None:
            raise KqlEngineError(
                f'invalid connection_str, connection_str pattern "{(alias or database_name)}@{cluster_friendly_name}" '
                f'can be used only after a previous connection was established to cluster "{cluster_friendly_name}"'
            )
        cluster_name = cluster_engine.get_cluster_name()
        conn_dict = { 
            ConnStrKeys.DATABASE: database_name,
            ConnStrKeys.CLUSTER: cluster_name,
            ConnStrKeys.ALIAS: alias,
            ConnStrKeys.CLUSTER_FRIENDLY_NAME: cluster_friendly_name
        }
        
        return KustoEngine(conn_dict, user_ns, **options, conn_class=Connection)

            
    @classmethod
    def _save_and_set_current_engine(cls, engine:Engine, name:str=None)->Engine:
        "Save engine if new. Set as current engine"

        id = engine.get_id()
        cls._current_engine = cls._engine_by_id.get(id) or engine
        cls._engine_by_id[id] = cls._current_engine

        name = name or engine.get_conn_name()
        cls._engine_by_id[name] = cls._current_engine

        cls._id_by_name[name] = id
        cls._id_by_name[engine.bind_url] = id
        return cls._current_engine


    @classmethod
    def get_engine_by_name(cls, name:str)->Engine:
        id = cls._id_by_name.get(name, name)
        return cls._engine_by_id.get(id)


    @classmethod
    def get_engine(cls, conn_str:str, user_ns:dict, **options)->Engine:
        "Sets the current engine based on connection string"
        "If engine can't be found it is created "
        if conn_str:
            # either exist or create a new one
            cls._current_engine = cls.get_engine_by_name(conn_str)
            if cls._current_engine is None:
                engine = cls._create_engine(conn_str, user_ns, **options)
                cls._save_and_set_current_engine(engine)

        if not cls._current_engine:
            raise ConnectionError("No _current connection set yet.")
        cls._last_current_by_engine_class[cls._current_engine.__class__.__name__] = cls._current_engine        
        return cls._current_engine


    @classmethod
    def _get_sorted_name_list(cls)->List[str]:
        "returns only alias@cluster list"
        return [name for name in sorted(cls._id_by_name) if name.find("@") > 0 and name.find("://") < 0]


    @classmethod
    def get_connection_list_by_schema(cls, uri_schema_name:str)->List[str]:
        "returns alias@cluster list that are filtered by schema name"
        uri_schema_name = uri_schema_name.lower()
        return [name for name in cls._get_sorted_name_list() if uri_schema_name in cls.get_engine_by_name(name).get_alt_uri_schema_names()]


    @classmethod
    def get_connection_list_formatted(cls)->List[str]:
        "returns only alias@cluster list, list is formatted, current connection is prefixed with an asterics"
        name_list = []
        for name in cls._get_sorted_name_list():
            asterics_current = '*' if cls.get_engine_by_name(name) == cls._current_engine else ' '
            name_list.append(f" {asterics_current} {name}")
        return name_list


    @classmethod
    def get_connections_info(cls, name_filter_list:List[str]=None)->Dict[str,dict]:
        "returns connection info details per connection name"
        name_list = cls._get_sorted_name_list()
        for name in (name_filter_list or []):
            if name not in name_list:
                raise ValueError(f"connection: '{name}' not found")
        return {name: cls.get_engine_by_name(name).get_details() for name in name_list if (name_filter_list is None or name in name_filter_list)}


    @classmethod
    def get_current_connection_formatted(cls)->str:
        "returns current connection name formated"
        return f" * {cls._current_engine.get_conn_name()}"


    @classmethod
    def get_current_connection_name(cls)->str:
        "returns currennection name"
        return cls._current_engine.get_conn_name() if cls._current_engine else None

# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

import os


from .kql_engine import KqlEngineError
from .kusto_engine import KustoEngine
from .ai_engine import AppinsightsEngine
from .la_engine import LoganalyticsEngine
from .cache_engine import CacheEngine
from .display import Display
from .constants import ConnStrKeys


class ConnectionError(Exception):
    pass


class Connection(object):

    current = None
    connections = {}
    last_current_by_engine = {}

    _ENGINES = [KustoEngine, AppinsightsEngine, LoganalyticsEngine, CacheEngine]
    _ENGINE_MAP = {}
    
    n = e = None
    for e in _ENGINES:
        for n in e._ALT_URI_SCHEMA_NAMES:
            _ENGINE_MAP[n] = e


    @classmethod
    def _find_engine(cls, connect_str):
        if connect_str is not None:
            parts = connect_str.split("://", 1)
            if len(parts) == 2:
                uri_schema = parts[0].lower().replace("_", "").replace("-", "")
                return cls._ENGINE_MAP.get(uri_schema)
    

    # Object constructor
    def __init__(self, connect_str, user_ns:dict, **options):

        engine = self._find_engine(connect_str)
        # wasn't found in connection list, but maybe a kusto database connection
        if engine is None:
            if "@" in connect_str:
                engine = KustoEngine
            else:
                valid_prefixes_str = ", ".join([f"{s}://" for s in self._ENGINE_MAP.keys()])
                raise KqlEngineError(f"invalid connection_str, unknown <uri schema name>. valid uri schemas are: {valid_prefixes_str}")

        last_current = self.last_current_by_engine.get(engine.__name__)

        if engine != KustoEngine:
            conn_engine = engine(connect_str, user_ns, **options, current =  last_current,)
        else:
            if "://" in connect_str:
                if last_current:
                    last_cluster_friendly_name = last_current.get_cluster_friendly_name()
                    last_current = self.connections.get(f"@{last_cluster_friendly_name}")
                # TODO: if already exist, not need to create a new one, root one each time, will make some of cluster kind sso 
                cluster_conn_engine = engine(connect_str, user_ns, **options, current = last_current)

                cluster_friendly_name = cluster_conn_engine.get_cluster_friendly_name()
                Connection._set_current(cluster_conn_engine, conn_name=f"@{cluster_friendly_name}")
                database_name = cluster_conn_engine.get_database()
                alias = cluster_conn_engine.get_alias()
            elif "@" in connect_str:
                database_name, cluster_friendly_name = connect_str.split("@", 1)
                alias = None
                if len(database_name) < 1:
                    raise KqlEngineError(f"invalid connection_str, key {ConnStrKeys.DATABASE} cannot be empty")
            else:
                raise KqlEngineError(f"invalid connection_str: {connect_str}")
            conn_engine = Connection._new_kusto_database_engine(database_name, cluster_friendly_name, alias, user_ns, **options)

        if options.get("use_cache") and engine != CacheEngine:
            conn_engine = CacheEngine(conn_engine, user_ns, last_current, **options)
        Connection._set_current(conn_engine)


    @classmethod
    def _new_kusto_database_engine(cls, database_name, cluster_friendly_name, alias, user_ns: dict, **options):
        if cluster_friendly_name in cls._ENGINE_MAP.keys():
            raise KqlEngineError(
                'invalid connection_str, connection_str pattern "database@cluster" cannot be used for "appinsights", "loganalytics" and "cache"'
            )
        cluster_conn_name = f"@{cluster_friendly_name}"
        cluster_conn = cls.connections.get(cluster_conn_name)
        if cluster_conn is None:
            raise KqlEngineError(
                'invalid connection_str, connection_str pattern "database@cluster" can be used only after a previous connection was established to a cluster'
            )
        cluster_name = cluster_conn.get_cluster()
        details = { 
            ConnStrKeys.DATABASE: database_name,
            ConnStrKeys.CLUSTER: cluster_name,
            ConnStrKeys.ALIAS: alias,
            "cluster_friendly_name": cluster_friendly_name
        }
        
        return KustoEngine(details, user_ns, **options, conn_class=Connection)


    @classmethod
    def _set_current(cls, conn_engine, conn_name=None):
        # already exist
        if cls.connections.get(conn_engine.bind_url):
            Connection.current = cls.connections[conn_engine.bind_url]
        # new connection
        else:
            Connection.current = conn_engine
            name = conn_name or conn_engine.get_conn_name()
            cls.connections[name] = conn_engine
            cls.connections[conn_engine.bind_url] = conn_engine


    @classmethod
    def get_connection_by_name(cls, name):
        return cls.connections.get(name)


    @classmethod
    def get_connection(cls, descriptor, user_ns, **options):
        "Sets the current database connection"
        if descriptor:
            if isinstance(descriptor, Connection):
                cls.current = descriptor
            else:
                # either exist or create a new one
                cls.current = cls.connections.get(descriptor) or Connection(descriptor, user_ns, **options).current
        elif not cls.current:
            raise ConnectionError("No current connection set yet.")
        cls.last_current_by_engine[cls.current.__class__.__name__] = cls.current        
        return cls.current


    @classmethod
    def connection_list(cls):
        return [k for k in sorted(cls.connections) if not k.startswith("@") and cls.connections[k].bind_url != k]

    @classmethod
    def get_connection_list_by_schema(cls, uri_schema_name):
        prefix = f"{uri_schema_name}://"
        return [k for k in Connection.connection_list() if cls.connections[k].bind_url.startswith(prefix)]


    @classmethod
    def get_connection_list_formatted(cls):
        result = []
        for key in Connection.connection_list():
            if cls.connections[key] == cls.current:
                template = " * {}"
            else:
                template = "   {}"
            result.append(template.format(key))
        return result


    @classmethod
    def get_current_connection_formatted(cls):
        return f" * {cls.current.get_conn_name()}"


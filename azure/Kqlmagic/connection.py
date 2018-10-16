#-------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
#--------------------------------------------------------------------------

import os
from Kqlmagic.kql_engine import KqlEngineError
from Kqlmagic.kusto_engine import KustoEngine
from Kqlmagic.ai_engine import AppinsightsEngine
from Kqlmagic.la_engine import LoganalyticsEngine
from Kqlmagic.cache_engine import CacheEngine
from Kqlmagic.display import Display


class ConnectionError(Exception):
    pass


class Connection(object):
    current = None
    connections = {}
    last_current_by_engine = {}

    _ENGINES = [KustoEngine, AppinsightsEngine, LoganalyticsEngine, CacheEngine]
    _ENGINE_MAP = {}
    for e in _ENGINES:
        for n in e._ALT_URI_SCHEMA_NAMES:
            _ENGINE_MAP[n] = e

    @classmethod
    def _get_engine(cls, connect_str):
        if connect_str is not None:
            parts = connect_str.split("://", 1)
            if len(parts) == 2:
                return  cls._ENGINE_MAP.get(parts[0].lower())

    @classmethod
    def tell_format(cls, connect_str):
        engine = cls._get_engine(connect_str)
        engines = [engine] if engine is not None else cls._ENGINES
        strs = [e.tell_format() for e in engines]
        lsts = []
        for e in engines:
            lsts.extend(Connection.get_connection_list_by_schema(e._URI_SCHEMA_NAME))
        msg = """connection string, examples:{0}
               or an existing connection: {1}
                   """.format(
            ''.join(strs), str(lsts)
        )
        return msg

    # Object constructor
    def __init__(self, connect_str, **kwargs):

        engine = self._get_engine(connect_str)
        # wasn't found in connection list, but maybe a kusto database connection
        if engine is None:
            if "@" in connect_str:
                engine = KustoEngine
            else:
                valid_prefixes_str = ", ".join(["{0}://".format(s) for s in self._ENGINE_MAP.keys()])
                raise KqlEngineError('invalid connection_str, unknown <uri schema name>. valid prefixes are: {0}'.format(valid_prefixes_str))

        last_current = self.last_current_by_engine.get(engine.__name__)

        if engine != KustoEngine:
            conn_engine = engine(connect_str, last_current)
        else:
            if "://" in connect_str:
                if last_current:
                    last_cluster_name = last_current.get_cluster()
                    last_current = self.connections.get("@" + last_cluster_name)
                cluster_conn_engine = engine(connect_str, last_current)
                cluster_name = cluster_conn_engine.get_cluster()
                Connection._set_current(cluster_conn_engine, conn_name="@" + cluster_name)
                database_name = cluster_conn_engine.get_database()
                alias = cluster_conn_engine.get_alias()
            else:
                database_name, cluster_name = conn_name.split("@")
                alias = None
            conn_engine = Connection._get_kusto_database_engine(database_name, cluster_name, alias)

        if kwargs.get('use_cache') and engine != CacheEngine:
            conn_engine = CacheEngine(conn_engine, last_current)
        Connection._set_current(conn_engine)

    @classmethod
    def _get_kusto_database_engine(cls, database_name, cluster_name, alias):
        if cluster_name in cls._ENGINE_MAP.keys():
            raise KqlEngineError(
                'invalid connection_str, connection_str pattern "database@cluster" cannot be used for "appinsights", "loganalytics" and "cache"'
            )
        cluster_conn_name = "@" + cluster_name
        cluster_conn = cls.connections.get(cluster_conn_name)
        if cluster_conn is None:
            raise KqlEngineError(
                'invalid connection_str, connection_str pattern "database@cluster" can be used only after a previous connection was established to a cluster'
            )
        details = {"database_name": database_name, "cluster_name":cluster_name, "alias": alias}
        return KustoEngine(details, conn_class=Connection)

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
    def get_connection(cls, descriptor, **kwargs):
        "Sets the current database connection"
        if descriptor:
            if isinstance(descriptor, Connection):
                cls.current = descriptor
            else:
                # either exist or create a new one
                cls.current = cls.connections.get(descriptor) or Connection(descriptor, **kwargs).current
        elif not cls.current:
            raise ConnectionError("No current connection set yet.")
        cls.last_current_by_engine[cls.current.__class__.__name__] = cls.current
        return cls.current

    @classmethod
    def connection_list(cls):
        return [k for k in sorted(cls.connections) if not k.startswith("@") and cls.connections[k].bind_url != k]

    @classmethod
    def get_connection_list_by_schema(cls, uri_schema_name):
        prefix = uri_schema_name + "://"
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
        return " * " + cls.current.get_conn_name()

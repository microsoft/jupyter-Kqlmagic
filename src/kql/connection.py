import os
from kql.kusto_engine import KustoEngine
from kql.ai_engine import AppinsightsEngine
from kql.la_engine import LoganalyticsEngine
from kql.display import Display

class ConnectionError(Exception):
    pass


class Connection(object):
    current = None
    connections = {}
    last_current_by_engine = {}

    @classmethod
    def _tell_format(cls, engine1 = None, engine2 = None, engine3 = None):
        str1 = engine1.tell_format() if engine1 else ''
        str2 = engine2.tell_format() if engine2 else ''
        str3 = engine2.tell_format() if engine3 else ''
        lst1 = Connection.get_connection_list_by_schema(engine1.schema) if engine1 else []
        lst2 = Connection.get_connection_list_by_schema(engine2.schema) if engine2 else []
        lst3 = Connection.get_connection_list_by_schema(engine3.schema) if engine3 else []
        msg = """kql magic format requires connection info, examples:{0}{1}{2}
               or an existing connection: {3}
                   """.format( str1, str2, str3, str(lst1 + lst2 + lst3))
        return msg

    @classmethod
    def tell_format(cls, connect_str=None):
        if connect_str.startswith('kusto://'):
            return Connection.tell_format(KustoEngine)
        elif connect_str.startswith('appinsights://'):
            return Connection.tell_format(AppinsightsEngine)
        elif connect_str.startswith('loganalytics://'):
            return Connection.tell_format(LoganalyticsEngine)
        else:
            return Connection.tell_format(KustoEngine, AppinsightsEngine, LoganalyticsEngine)

    # Object constructor
    def __init__(self, connect_str=None):
        if connect_str.startswith('kusto://'):
            engine = KustoEngine
        elif connect_str.startswith('appinsights://'):
            engine = AppinsightsEngine
        elif connect_str.startswith('loganalytics://'):
            engine = LoganalyticsEngine
        elif connect_str.startswith('loganalytics://'):
            engine = LoganalyticsEngine
        elif '@' in connect_str:
            raise ConnectionError('Connection not found.')
        else:
            raise KqlEngineError('invalid connection_str, unknown schema. valid schemas are: "kusto://", "appinsights://" and "loganalytics://"')

        last_current = self.last_current_by_engine.get(engine.__name__)

        conn_engine = engine(connect_str, last_current)

        Connection.current = conn_engine
        if conn_engine.bind_url:
            # already exist
            if self.connections.get(conn_engine.bind_url):
                Connection.current = self.connections[conn_engine.bind_url]
            # new connection
            else:
                name = self.assign_name(conn_engine)
                # rename the name according the asiigned name
                # conn_engine.name = name
                self.connections[name] = conn_engine
                self.connections[conn_engine.bind_url] = conn_engine


    @classmethod
    def get_connection(cls, descriptor):
        "Sets the current database connection"

        if descriptor:
            if isinstance(descriptor, Connection):
                cls.current = descriptor
            else:
                # either exist or create a new one
                cls.current = cls.connections.get(descriptor) or Connection(descriptor).current
        else:
            if not cls.current:
                if not os.getenv('KQL_CONNECTION_STR'):
                    raise ConnectionError('No current connection set yet.')
                cls.current = Connection(os.getenv('KQL_CONNECTION_STR')).current
        cls.last_current_by_engine[cls.current.__class__.__name__] = cls.current
        return cls.current

    @classmethod
    def assign_name(cls, engine):
        "Assign a unique name for the connection"

        incrementer = 1
        name = core_name = engine.get_name()
        while name in cls.connections:
            name = '{0}_{1}'.format(core_name, incrementer)
            incrementer += 1
        return name

    @classmethod
    def connection_list(cls):
        return [k for k in sorted(cls.connections) if cls.connections[k].bind_url != k]


    @classmethod
    def get_connection_list_by_schema(cls, schema):
        return [k for k in sorted(cls.connections) if cls.connections[k].bind_url != k and cls.connections[k].bind_url.startswith(schema)]


    @classmethod
    def get_connection_list_formatted(cls):
        result = []
        for key in Connection.connection_list():
            if cls.connections[key] == cls.current:
                template = ' * {}'
            else:
                template = '   {}'
            result.append(template.format(key))
        return result

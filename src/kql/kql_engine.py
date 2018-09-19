from kql.kql_proxy import KqlResponse


class KqlEngine(object):

    # Object constructor
    def __init__(self):
        self.bind_url = None
        self.cluster_url = None
        self.database_name = None
        self.cluster_name = None
        self.client = None
        self.options = {}

        self.validated = None

    def __eq__(self, other):
        return self.bind_url and self.bind_url == other.bind_url

    def is_validated(self):
        return self.validated == True

    def set_validation_result(self, result):
        self.validated = result == True

    def get_database(self):
        if not self.database_name:
            raise KqlEngineError("Database is not defined.")
        return self.database_name

    def get_cluster(self):
        if not self.cluster_name:
            raise KqlEngineError("Cluster is not defined.")
        return self.cluster_name

    def get_conn_name(self):
        if self.database_name and self.cluster_name:
            return "{0}@{1}".format(self.database_name, self.cluster_name)
        else:
            raise KqlEngineError("Database and/or cluster is not defined.")

    def get_client(self):
        return self.client

    def execute(self, query, user_namespace=None, **kwargs):
        if query.strip():
            client = self.get_client()
            if not client:
                raise KqlEngineError("Client is not defined.")
            response = client.execute(self.get_database(), query, accept_partial_results=False, timeout=None)
            # print(response.json_response)
            return KqlResponse(response, **kwargs)

    def validate(self, **kwargs):
        client = self.get_client()
        if not client:
            raise KqlEngineError("Client is not defined.")
        query = "range c from 1 to 10 step 1 | count"
        response = client.execute(self.get_database(), query, accept_partial_results=False, timeout=None)
        # print(response.json_response)
        table = KqlResponse(response, **kwargs).tables[0]
        if table.rowcount() != 1 or table.colcount() != 1 or [r for r in table.fetchall()][0][0] != 10:
            raise KqlEngineError("Client failed to validate connection.")

class KqlEngineError(Exception):
    """Generic error class."""

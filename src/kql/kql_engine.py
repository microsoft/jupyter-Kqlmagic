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

    def is_validated (self):
        return self.validated == True

    def set_validation_result (self, result):
        self.validated = result == True

    def get_database(self):
        if not self.database_name:
            raise KqlEngineError("Database is not defined.")
        return self.database_name

    def get_cluster(self):
        if not self.cluster_name:
            raise KqlEngineError("Cluster is not defined.")
        return self.cluster_name

    def get_name(self):
        if self.database_name and self.cluster_name:
            return '{0}@{1}'.format(self.database_name, self.cluster_name)
        else:
            raise KqlEngineError("Database and/or cluster is not defined.")

    def execute(self, code, user_namespace = None):
        if code.strip():
            if not self.client:
                raise KqlEngineError("Client is not defined.")
            response = self.client.execute(self.get_database(), code, False)
            return KqlResponse(response)


class KqlEngineError(Exception):
    """Generic error class."""


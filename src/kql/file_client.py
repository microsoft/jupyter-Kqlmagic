#-------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
#--------------------------------------------------------------------------

from kql.kusto_client import KustoResponse
import hashlib
import json
import os


class FileClient(object):
    """
    """

    def __init__(
        self
    ):
        """
        File Client constructor.

        Parameters
        ----------
        cluster_folder : str
            folder that contains all the databse_folders that contains the query result files
        """
        ip = get_ipython()
        root_path = ip.starting_dir.replace("\\", "/")
        self.files_folder = root_path + "/" + ip.run_line_magic("config", "Kqlmagic.file_schema_folder_name")


    def _get_query_hash_filename(self, query):
        lines = [l.replace('\r', '').replace('\t',' ').strip() for l in query.split('\n')]
        q_lines = []
        for line in lines:
            if not line.startswith('//'):
                idx = line.find(' //')
                q_lines.append(line[:idx if idx >= 0 else len(line)])
        return 'q_' + hashlib.sha1(bytes(''.join(q_lines), 'utf-8')).hexdigest() + '.json'


    def _get_file_path(self, query, database_at_cluster):
        """ get the file name from the query string.
        if query string ends with the '.json' extension it returns the string
        otherwise it computes it from the query
        """
        file_name = query if query.strip().endswith('.json') else self._get_query_hash_filename(query)
        database_name, cluster_name = database_at_cluster.split('_at_')
        folder_path = self._get_folder_path(database_name, cluster_name)
        filen_path = folder_path + '/' + file_name
        return filen_path.replace('\\', '/')


    def _get_folder_path(self, database_name, cluster_name, **kwargs):
        if not os.path.exists(self.files_folder):
            os.makedirs(self.files_folder)
        cluster_folder_name = self.files_folder + "/" + cluster_name
        if not os.path.exists(cluster_folder_name):
            os.makedirs(cluster_folder_name)
        database_folder_name = cluster_folder_name + "/" + database_name
        if not os.path.exists(database_folder_name):
            os.makedirs(database_folder_name)
        return database_folder_name


    def _get_endpoint_version(self, json_response):
        try:
            tables_num = json_response["Tables"].__len__()
            return "v1"
        except:
            return "v2"


    def execute(self, database_at_cluster, query, **kwargs):
        """Executes a query or management command.
        :param str database_at_cluster: name of database and cluster that a folder will be derived that contains all the files with the query results for this specific database.
        :param str query: Query to be executed.
        """
        file_path = self._get_file_path(query, database_at_cluster)
        str_response = open(file_path, 'r').read()
        json_response = json.loads(str_response)
        endpoint_version = self._get_endpoint_version(json_response)
        return KustoResponse(json_response, endpoint_version)


    def execute_query(self, database_at_cluster, query, **kwargs):
        """Executes a query.
        :param str database_at_cluster: name of database and cluster that a folder will be derived that contains all the files with the query results for this specific database.
        :param str query: Query to be executed.
        """
        return self.execute(database_at_cluster, query, **kwargs)


    def execute_mgmt(self, database_at_cluster, query, **kwargs):
        """Executes a management command.
        :param str database_at_cluster: name of database and cluster that a folder will be derived that contains all the files with the query results for this specific database.
        :param str query: Query to be executed.
        """
        return self.execute(database_at_cluster, query, **kwargs)

    def save(self, database, cluster, query, result, **kwargs):
        """Executes a query or management command.
        :param str database_at_cluster: name of database and cluster that a folder will be derived that contains all the files with the query results for this specific database.
        :param str query: Query to be executed.
        """
        database_at_cluster = database + '_at_' + cluster
        file_path = self._get_file_path(query, database_at_cluster)
        outfile = open(file_path, "w")
        outfile.write(json.dumps(result.json_response))
        outfile.flush()
        outfile.close()
        return file_path


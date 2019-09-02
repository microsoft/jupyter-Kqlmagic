# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

from .constants import Constants
from .my_utils import get_valid_filename, adjust_path
from .kql_response import KqlQueryResponse, KqlSchemaResponse
import hashlib
import json
import os


class CacheClient(object):
    """
    """

    def __init__(self):
        """
        File Client constructor.

        Parameters
        ----------
        cluster_folder : str
            folder that contains all the database_folders that contains the query result files
        """
        ip = get_ipython()  # pylint: disable=undefined-variable
        root_path = ip.starting_dir
        cache_folder_name = ip.run_line_magic("config", f"{Constants.MAGIC_CLASS_NAME}.cache_folder_name")
        self.files_folder = adjust_path(f"{root_path}/{cache_folder_name}")


    @property
    def data_source(self):
        return self.files_folder

    def _get_query_hash_filename(self, query):
        lines = [l.replace("\r", "").replace("\t", " ").strip() for l in query.split("\n")]
        q_lines = []
        for line in lines:
            if not line.startswith("//"):
                idx = line.find(" //")
                q_lines.append(line[: idx if idx >= 0 else len(line)])
        return "q_" + hashlib.sha1(bytes("".join(q_lines), "utf-8")).hexdigest() + ".json"


    def _get_file_path(self, query, database_at_cluster, cache_folder):
        """ 
        get the file name from the query string.

        if query string ends with the '.json' extension it returns the string
        otherwise it computes it from the query
        """
        file_name = query if query.strip().endswith(".json") else self._get_query_hash_filename(query)
        folder_path = self._get_folder_path(database_at_cluster, cache_folder=cache_folder)
        file_path = f"{folder_path}/{file_name}"
        return adjust_path(file_path)


    def _get_folder_path(self, database_at_cluster, cache_folder=None):
        if "_at_" in database_at_cluster:
            database_at_cluster = "_".join(database_at_cluster.split())
            database_name, cluster_name = database_at_cluster.split("_at_")[:2]

            if not os.path.exists(self.files_folder):
                os.makedirs(self.files_folder)
            folder_path = self.files_folder
            if  cache_folder is not None:
                folder_path += "/" + adjust_path(cache_folder)
                if not os.path.exists(folder_path):
                    os.makedirs(folder_path)
            folder_path += "/" + get_valid_filename(cluster_name)
            if not os.path.exists(folder_path):
                os.makedirs(folder_path)
            folder_path += "/" + get_valid_filename(database_name)
        else:
            folder_path = database_at_cluster

        folder_path = adjust_path(folder_path)

        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
        return folder_path


    def _get_endpoint_version(self, json_response):
        try:
            tables_num = json_response["Tables"].__len__()  # pylint: disable=W0612
            return "v1"
        except:
            return "v2"


    def execute(self, database_at_cluster, query, **options):
        """
        Executes a query or management command.

        :param str database_at_cluster: name of database and cluster that a folder will be derived that contains all the files with the query results for this specific database.
        :param str query: Query to be executed or a json file with query results.
        """
        file_path = self._get_file_path(query, database_at_cluster, cache_folder=options.get("use_cache"))
        str_response = open(file_path, "r").read()
        json_response = json.loads(str_response)
        if query.startswith(".") and json_response.get("tables") is not None:
            return KqlSchemaResponse(json_response)
        else:
            endpoint_version = self._get_endpoint_version(json_response)
            return KqlQueryResponse(json_response, endpoint_version)


    def save(self, result, conn, query, file_path=None, filefolder=None, **options):
        """
        Executes a query or management command.

        :param str database_at_cluster: name of database and cluster that a folder will be derived that contains all the files with the query results for this specific database.
        :param str query: Query to be executed.
        """

        if filefolder is not None:
            file_path = f"{filefolder}/{self._get_query_hash_filename(query)}"
        if file_path is not None:
            file_path = adjust_path(file_path)
            parts = file_path.split("/")
            folder_parts = []
            for part in parts[:-1]:
                folder_parts.append(part)
                folder_name = "/".join(folder_parts)
                if not os.path.exists(folder_name):
                    os.makedirs(folder_name)
        else:
            database_friendly_name = conn.get_database_friendly_name()
            cluster_friendly_name = conn.get_cluster_friendly_name()
            file_path = self._get_file_path(query, f"{database_friendly_name}_at_{cluster_friendly_name}", cache_folder=options.get("cache"))
        outfile = open(file_path, "w")
        outfile.write(json.dumps(result.json_response))
        outfile.flush()
        outfile.close()
        return file_path

# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

from typing import Any, Union, Dict, List
import hashlib
import json
import os
import shutil


from .constants import Constants
from .my_utils import get_valid_filename_with_spaces, adjust_path, convert_to_common_path_obj, json_dumps 
from .kql_response import KqlQueryResponse, KqlSchemaResponse
from .ipython_api import IPythonAPI
from .kql_client import KqlClient
from .kql_engine import KqlEngine


class CacheClient(KqlClient):
    """
    """

    @staticmethod
    def abs_cache_folder(folder_name:str=None, **options)->str:
        root_path = IPythonAPI.get_ipython_root_path(**options)
        cache_folder_name = options.get("cache_folder_name")
        cache_folder_name = f"{Constants.MAGIC_CLASS_NAME_LOWER}/{cache_folder_name}"
        if options.get("temp_folder_location") == "user_dir":
            # app that has a free/tree build server, are not supporting directories athat starts with a dot
            cache_folder_name = f".{cache_folder_name}"
        folder_name_path = f"/{folder_name}" if folder_name else ""
        files_folder = adjust_path(f"{root_path}/{cache_folder_name}{folder_name_path}")
        return files_folder


    @staticmethod
    def remove_cache(folder_name:str=None, **options)->bool:
        cache_folder = CacheClient.abs_cache_folder(folder_name=folder_name, **options)
        if os.path.exists(cache_folder):
            shutil.rmtree(cache_folder)
            return True
        else:
            return False

    @staticmethod
    def list_cache(**options)->List[str]:
        cache_container = CacheClient.abs_cache_folder(**options)
        if os.path.exists(cache_container):
            return os.listdir(cache_container)
        else:
            return []


    @staticmethod
    def create_or_attach_cache(folder_name:str=None, **options)->bool:
        cache_folder = CacheClient.abs_cache_folder(folder_name=folder_name, **options)
        if not os.path.exists(cache_folder):
            os.makedirs(cache_folder)
            return True
        else:
            return False


    def __init__(self, **options)->None:
        """
        File Client constructor.

        Parameters
        ----------
        cluster_folder : str
            folder that contains all the database_folders that contains the query result files
        """

        super(CacheClient, self).__init__()
        self.files_folder = CacheClient.abs_cache_folder(**options)


    @property
    def data_source(self)->str:
        return self.files_folder


    def _get_query_hash_filename(self, query:str)->str:
        lines = [line.replace("\r", "").replace("\t", " ").strip() for line in query.split("\n")]
        q_lines = []
        for line in lines:
            if not line.startswith("//"):
                idx = line.find(" //")
                q_lines.append(line[: idx if idx >= 0 else len(line)])
        return f"q_{hashlib.sha1(bytes(''.join(q_lines), 'utf-8')).hexdigest()}.json"


    def _get_file_path(self, query:str, database_at_cluster:str, cache_folder:str)->str:
        """ 
        get the file name from the query string.

        if query string ends with the '.json' extension it returns the string
        otherwise it computes it from the query
        """

        file_name = query if query.strip().endswith(".json") else self._get_query_hash_filename(query)
        folder_path = self._get_folder_path(database_at_cluster, cache_folder=cache_folder)
        file_path = f"{folder_path}/{file_name}"
        return adjust_path(file_path)


    def _get_folder_path(self, database_at_cluster:str, cache_folder:str=None)->str:
        if "_at_" in database_at_cluster:
            database_at_cluster = "_".join(database_at_cluster.split())
            database_name, cluster_name = database_at_cluster.split("_at_")[:2]

            if not os.path.exists(self.files_folder):
                os.makedirs(self.files_folder)
            folder_path = self.files_folder
            if cache_folder is not None:
                folder_path = adjust_path(f"{folder_path}/{cache_folder}")
                if not os.path.exists(folder_path):
                    os.makedirs(folder_path)
            folder_path = adjust_path(f"{folder_path}/{get_valid_filename_with_spaces(cluster_name)}")
            if not os.path.exists(folder_path):
                os.makedirs(folder_path)
            folder_path = f"{folder_path}/{get_valid_filename_with_spaces(database_name)}"

        else:
            folder_path = database_at_cluster

        folder_path = adjust_path(folder_path)

        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
        return folder_path


    def _get_endpoint_version(self, json_response:Dict[str,Any])->str:
        try:
            tables_num = json_response["Tables"].__len__()  # pylint: disable=W0612
            return "v1"
        except:
            return "v2"


    def execute(self, database_at_cluster:str, query:str, **options)->Union[KqlSchemaResponse,KqlQueryResponse]:
        """
        Executes a query or management command.

        :param str database_at_cluster: name of database and cluster that a folder will be derived that contains all the files with the query results for this specific database.
        :param str query: Query to be executed or a json file with query results.
        """

        file_path = self._get_file_path(query, database_at_cluster, cache_folder=options.get("use_cache"))
        # collect this inormation, in case bug report will be generated
        KqlClient.last_query_info = {
            "request": {
                "file_path": file_path,
            },
            "response": {
                "status_code": 200,
            },
        }
        try:
            str_response = open(file_path, "r").read()
            json_response = json.loads(str_response)
            if query.startswith(".") and json_response.get("tables") is not None:
                return KqlSchemaResponse(json_response)
            else:
                endpoint_version = self._get_endpoint_version(json_response)
                return KqlQueryResponse(json_response, endpoint_version)
                
        except Exception as e:
            # collect this inormation, in case bug report will be generated
            self.last_query_info["response"]["status_code"] = 400  # pylint: disable=unsupported-assignment-operation, unsubscriptable-object
            self.last_query_info["response"]["error"] = str(e)  # pylint: disable=unsupported-assignment-operation, unsubscriptable-object
            raise e


    def save(self, result, engine:KqlEngine, query:str, file_path:str=None, filefolder:str=None, **options)->str:
        """
        Executes a query or management command.

        :param str database_at_cluster: name of database and cluster that a folder will be derived that contains all the files with the query results for this specific database.
        :param str query: Query to be executed.
        """
        if filefolder is not None:
            file_path = f"{filefolder}/{self._get_query_hash_filename(query)}"

        if file_path is not None:
            path_obj = convert_to_common_path_obj(file_path)

            parts = path_obj.get("path").split("/")
            folder_parts = []
            for part in parts[:-1]:
                folder_parts.append(part)
                folder_name = f'{path_obj.get("prefix")}/'.join(folder_parts)
                os_folder_name = adjust_path(folder_name)
                if not os.path.exists(os_folder_name):
                    os.makedirs(os_folder_name)
            file_path = adjust_path(file_path)
            
        else:
            database_friendly_name = engine.get_database_friendly_name()
            cluster_friendly_name = engine.get_cluster_friendly_name()
            file_path = self._get_file_path(query, f"{database_friendly_name}_at_{cluster_friendly_name}", cache_folder=options.get("cache"))
        outfile = open(file_path, "w")
        outfile.write(json_dumps(result.json_response))
        outfile.flush()
        outfile.close()
        return file_path

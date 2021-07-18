# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

from typing import Any, List, Dict, Union, Tuple


from .kql_response import KqlQueryResponse, KqlSchemaResponse
from .kql_proxy import KqlResponse
from .kql_client import KqlClient


class Engine(object):

    # Object constructor
    def __init__(self):
        pass


    @classmethod
    def get_alt_uri_schema_names(cls):
        raise NotImplementedError(cls.__class__.__name__ + ".get_alt_uri_schema_names")


    @classmethod
    def get_reserved_cluster_name(cls):
        raise NotImplementedError(cls.__class__.__name__ + ".get_reserved_cluster_name")


    @classmethod
    def get_mandatory_key(cls)->str:
        raise NotImplementedError(cls.__class__.__name__ + ".get_mandatory_key")


    @classmethod
    def get_valid_keys_combinations(cls)->List[List[str]]:
        raise NotImplementedError(cls.__class__.__name__ + ".get_valid_keys_combinations")


    @classmethod
    def get_uri_schema_name(cls):
        raise NotImplementedError(cls.__class__.__name__ + ".get_uri_schema_name")


    def obfuscate_parsed_conn(self)->Dict[str,str]:
        raise NotImplementedError(self.__class__.__name__ + ".obfuscate_parsed_conn")


    # collect datails, in case bug report will be generated
    def get_details(self)->Dict[str,Any]:
        raise NotImplementedError(self.__class__.__name__ + ".get_details")


    def __eq__(self, other)->bool:
        raise NotImplementedError(self.__class__.__name__ + ".__eq__")


    def get_alias(self)->str:
        raise NotImplementedError(self.__class__.__name__ + ".get_alias")


    def get_database_name(self)->str:
        raise NotImplementedError(self.__class__.__name__ + ".get_database_name")


    def get_client_database_name(self)->str:
        raise NotImplementedError(self.__class__.__name__ + ".get_client_database_name")


    def get_cluster_name(self)->str:
        raise NotImplementedError(self.__class__.__name__ + ".get_cluster_name")


    def get_cluster_friendly_name(self)->str:
        raise NotImplementedError(self.__class__.__name__ + ".get_cluster_friendly_name")


    def get_database_friendly_name(self)->str:
        raise NotImplementedError(self.__class__.__name__ + ".get_database_friendly_name")


    def get_id(self)->str:
        raise NotImplementedError(self.__class__.__name__ + ".get_id")


    def get_conn_name(self)->str:
        raise NotImplementedError(self.__class__.__name__ + ".get_conn_name")


    def get_deep_link(self, query:str, options:Dict[str,Any]={})->str:
        raise NotImplementedError(self.__class__.__name__ + ".get_deep_link")


    def createDatabaseFriendlyName(self, dname:str)->str:
        raise NotImplementedError(self.__class__.__name__ + ".createDatabaseFriendlyName")


    def createClusterFriendlyName(self, cname:str)->str:
        raise NotImplementedError(self.__class__.__name__ + ".createClusterFriendlyName")


    def get_client(self)->KqlClient:
        raise NotImplementedError(self.__class__.__name__ + ".get_client")


    def client_execute(self, query:str, user_namespace:Dict[str,Any]=None, database=None, **options)->Union[KqlQueryResponse, KqlSchemaResponse]:
        raise NotImplementedError(self.__class__.__name__ + ".client_execute")


    def execute(self, query:str, user_namespace:Dict[str,Any]=None, database=None, **options)->KqlResponse:
        raise NotImplementedError(self.__class__.__name__ + ".execute")


    def validate(self, **options)->None:
        raise NotImplementedError(self.__class__.__name__ + ".validate")
        

    def validate_database_name(self, **options)->None:
        raise NotImplementedError(self.__class__.__name__ + ".validate_database_name")


    def _get_databases_by_pretty_name(self, **options)->Tuple[List[str],Dict[str,str]]:
        raise NotImplementedError(self.__class__.__name__ + "._get_databases_by_pretty_name")


    def _parse_common_connection_str(self, conn_str:str, current, uri_schema_name:str, mandatory_key:str, keys_combinations:List[str], user_ns:Dict[str,Any])->Dict[str,str]:

        raise NotImplementedError(self.__class__.__name__ + "._parse_common_connection_str")

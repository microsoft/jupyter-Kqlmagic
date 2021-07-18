# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

from typing import Union
import uuid

from .kql_response import KqlQueryResponse, KqlSchemaResponse
from .aad_helper import AadHelper


class KqlClient(object):

    # collect this information, in case bug report will be generated
    last_query_info:dict = None

    _aad_helper:AadHelper

    _session_guid:str = str(uuid.uuid4())


    def __init__(self)->None:
        self._aad_helper = None


    def execute(self, id:str, query:str, accept_partial_results:bool=False, **options)->Union[KqlQueryResponse, KqlSchemaResponse]:
        raise NotImplementedError(self.__class__.__name__ + ".execute")

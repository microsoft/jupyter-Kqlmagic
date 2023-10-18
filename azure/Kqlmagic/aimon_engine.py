# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

from typing import Any, Dict


from .ai_engine import AppinsightsEngine
from .constants import Schema


class AimonEngine(AppinsightsEngine):

    # Constants
    # ---------
    _URI_SCHEMA_NAME = Schema.AIMON  # no spaces, underscores, and hyphe-minus, because they are ignored in parser
    _ALT_URI_SCHEMA_NAME = Schema.AIMON  # no spaces, underscores, and hyphe-minus, because they are ignored in parser
    _DOMAIN = "apps"
    
    _DATA_SOURCE = "https://api.aimon.applicationinsights.io"
 
    _ALT_URI_SCHEMA_NAMES = [_URI_SCHEMA_NAME]
    _RESERVED_CLUSTER_NAME = _URI_SCHEMA_NAME
    
    # Class methods
    # -------------

            
    # Instance methods
    # ----------------

    def __init__(self, conn_str:str, user_ns:Dict[str,Any], current:AppinsightsEngine=None, **options)->None:
        super(AimonEngine, self).__init__(conn_str, user_ns, current, **options)

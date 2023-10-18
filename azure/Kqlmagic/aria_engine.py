# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

from typing import Any, Dict


from .kusto_engine import KustoEngine
from .constants import ConnStrKeys, Schema


class AriaEngine(KustoEngine):

    # Constants
    # ---------

    _URI_SCHEMA_NAME = Schema.ARIA  # no spaces, underscores, and hyphe-minus, because they are ignored in parser

    _ALT_URI_SCHEMA_NAMES = [_URI_SCHEMA_NAME]
    _RESERVED_CLUSTER_NAME = _URI_SCHEMA_NAME

    _DEFAULT_CLUSTER_NAME = "https://kusto.aria.microsoft.com"

    _VALID_KEYS_COMBINATIONS = [
        [ConnStrKeys.DATABASE, ConnStrKeys.ALIAS, ConnStrKeys.TENANT, ConnStrKeys.AAD_URL, ConnStrKeys.CLIENTID, ConnStrKeys.CLIENTSECRET],
        [ConnStrKeys.DATABASE, ConnStrKeys.ALIAS, ConnStrKeys.TENANT, ConnStrKeys.AAD_URL, ConnStrKeys.CLIENTID, ConnStrKeys.CERTIFICATE, ConnStrKeys.CERTIFICATE_THUMBPRINT],
        [ConnStrKeys.DATABASE, ConnStrKeys.ALIAS, ConnStrKeys.TENANT, ConnStrKeys.AAD_URL, ConnStrKeys.CLIENTID, ConnStrKeys.CODE],
        [ConnStrKeys.DATABASE, ConnStrKeys.ALIAS, ConnStrKeys.TENANT, ConnStrKeys.AAD_URL,                       ConnStrKeys.CODE],
        [ConnStrKeys.DATABASE, ConnStrKeys.ALIAS, ConnStrKeys.TENANT, ConnStrKeys.AAD_URL, ConnStrKeys.CLIENTID, ConnStrKeys.USERNAME, ConnStrKeys.PASSWORD],
        [ConnStrKeys.DATABASE, ConnStrKeys.ALIAS, ConnStrKeys.TENANT, ConnStrKeys.AAD_URL,                       ConnStrKeys.USERNAME, ConnStrKeys.PASSWORD],
        [ConnStrKeys.DATABASE, ConnStrKeys.ALIAS,                                                                ConnStrKeys.ANONYMOUS],
    ]

    _VALID_KEYS_COMBINATIONS_NEW = [
        {
            "must": [ConnStrKeys.DATABASE, ConnStrKeys.CLIENTID, ConnStrKeys.CLIENTSECRET],
            "extra": [],
            "optional": [ConnStrKeys.ALIAS, ConnStrKeys.TENANT, ConnStrKeys.AAD_URL]
        },
        {
            "must": [ConnStrKeys.DATABASE, ConnStrKeys.CLIENTID, ConnStrKeys.CERTIFICATE, ConnStrKeys.CERTIFICATE_THUMBPRINT],
            "extra": [],
            "optional": [ConnStrKeys.ALIAS, ConnStrKeys.TENANT, ConnStrKeys.AAD_URL]
        },
        {
            "must": [ConnStrKeys.DATABASE, ConnStrKeys.CODE],
            "extra": [ConnStrKeys.CLIENTID, ConnStrKeys.USERNAME],
            "optional": [ConnStrKeys.ALIAS, ConnStrKeys.TENANT, ConnStrKeys.AAD_URL]
        },
        {
            "must": [ConnStrKeys.DATABASE, ConnStrKeys.USERNAME, ConnStrKeys.PASSWORD],
            "extra": [ConnStrKeys.CLIENTID],
            "optional": [ConnStrKeys.ALIAS, ConnStrKeys.TENANT, ConnStrKeys.AAD_URL, ConnStrKeys.CLIENTID]
        },
        {
            "must": [ConnStrKeys.DATABASE, ConnStrKeys.ANONYMOUS],
            "extra": [],
            "optional": [ConnStrKeys.ALIAS]
        }
    ]

    # Class methods
    # -------------

    # Instance methods
    # ----------------

    def __init__(self, conn_str:str, user_ns:Dict[str,Any], current:KustoEngine=None, conn_class=None, **options)->Any:
        super(AriaEngine, self).__init__(conn_str, user_ns, current, conn_class, **options)

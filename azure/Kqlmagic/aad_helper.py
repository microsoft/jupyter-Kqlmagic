# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

"""An abstract module to acquire tokens from AAD.
"""


class AadHelper(object):

    def __init__(self, kcsb, default_clientid, msal_client_app=None, msal_client_app_sso=None, **options):
        pass

    def get_details(self):
        raise NotImplementedError(self.__class__.__name__ + ".get_details")


    def acquire_token(self):
        """Acquire tokens from AAD."""
        raise NotImplementedError(self.__class__.__name__ + ".acquire_token")

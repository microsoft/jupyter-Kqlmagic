#-------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
#--------------------------------------------------------------------------

""" A module to acquire tokens from AAD.
"""

from enum import Enum, unique
from datetime import timedelta, datetime

# import webbrowser
from six.moves.urllib.parse import urlparse

import dateutil.parser
from adal import AuthenticationContext
from adal.constants import TokenResponseFields, OAuth2DeviceCodeResponseParameters
from kql.display import Display


class ConnKeysKCSB(object):
    """
    Object like dict, every dict[key] can be visited by dict.key
    """
    def __init__(self, conn_kv, data_source):
        self.conn_kv = conn_kv
        self.data_source = data_source
        self.translate_map =  {
            "authority_id" : "tenant", 
            "aad_user_id" : "username",
            "password" : "password",
            "application_client_id" : "clientid",
            "application_key" : "clientsecret",
            "application_certificate" : "certificate",
            "application_certificate_thumbprint" : "certificate_thumbprint",
        }

    def __getattr__(self, kcsb_attr_name):
        if kcsb_attr_name == "data_source":
            return self.data_source
        key = self.translate_map.get(kcsb_attr_name)
        return self.conn_kv.get(key)

@unique
class AuthenticationMethod(Enum):
    """Enum represnting all authentication methods available in Kusto with Python."""

    aad_username_password = "aad_username_password"
    aad_application_key = "aad_application_key"
    aad_application_certificate = "aad_application_certificate"
    aad_device_login = "aad_device_login"


class _MyAadHelper(object):
    def __init__(self, kcsb, default_cleintid):
        authority = kcsb.authority_id or "common"
        self._kusto_cluster = "{0.scheme}://{0.hostname}".format(urlparse(kcsb.data_source))
        self._adal_context = AuthenticationContext("https://login.microsoftonline.com/{0}".format(authority))
        self._username = None
        if all([kcsb.aad_user_id, kcsb.password]):
            self._authentication_method = AuthenticationMethod.aad_username_password
            self._client_id = default_cleintid
            self._username = kcsb.aad_user_id
            self._password = kcsb.password
        elif all([kcsb.application_client_id, kcsb.application_key]):
            self._authentication_method = AuthenticationMethod.aad_application_key
            self._client_id = kcsb.application_client_id
            self._client_secret = kcsb.application_key
        elif all([kcsb.application_client_id, kcsb.application_certificate, kcsb.application_certificate_thumbprint]):
            self._authentication_method = AuthenticationMethod.aad_application_certificate
            self._client_id = kcsb.application_client_id
            self._certificate = kcsb.application_certificate
            self._thumbprint = kcsb.application_certificate_thumbprint
        else:
            self._authentication_method = AuthenticationMethod.aad_device_login
            self._client_id = default_cleintid

    def acquire_token(self):
        """Acquire tokens from AAD."""
        token = self._adal_context.acquire_token(self._kusto_cluster, self._username, self._client_id)
        if token is not None:
            expiration_date = dateutil.parser.parse(token[TokenResponseFields.EXPIRES_ON])
            if expiration_date > datetime.now() + timedelta(minutes=1):
                return self._get_header(token)
            if TokenResponseFields.REFRESH_TOKEN in token:
                token = self._adal_context.acquire_token_with_refresh_token(
                    token[TokenResponseFields.REFRESH_TOKEN], self._client_id, self._kusto_cluster
                )
                if token is not None:
                    return self._get_header(token)

        if self._authentication_method is AuthenticationMethod.aad_username_password:
            token = self._adal_context.acquire_token_with_username_password(self._kusto_cluster, self._username, self._password, self._client_id)
        elif self._authentication_method is AuthenticationMethod.aad_application_key:
            token = self._adal_context.acquire_token_with_client_credentials(self._kusto_cluster, self._client_id, self._client_secret)
        elif self._authentication_method is AuthenticationMethod.aad_device_login:
            # print(code[OAuth2DeviceCodeResponseParameters.MESSAGE])
            # webbrowser.open(code[OAuth2DeviceCodeResponseParameters.VERIFICATION_URL])
            # token = self._adal_context.acquire_token_with_device_code(
            #     self._kusto_cluster, code, self._client_id
            # )
            code = self._adal_context.acquire_user_code(self._kusto_cluster, self._client_id)
            url = code[OAuth2DeviceCodeResponseParameters.VERIFICATION_URL]
            device_code = code[OAuth2DeviceCodeResponseParameters.USER_CODE].strip()

            html_str = (
                """<!DOCTYPE html>
                <html><body>

                <!-- h1 id="user_code_p"><b>"""
                + device_code
                + """</b><br></h1-->

                <input  id="kql_MagicCodeAuthInput" type="text" readonly style="font-weight: bold; border: none;" size = '"""
                + str(len(device_code))
                + """' value='"""
                + device_code
                + """'>

                <button id='kql_MagicCodeAuth_button', onclick="this.style.visibility='hidden';kql_MagicCodeAuthFunction()">Copy code to clipboard and authenticate</button>

                <script>
                var kql_MagicUserCodeAuthWindow = null
                function kql_MagicCodeAuthFunction() {
                    /* Get the text field */
                    var copyText = document.getElementById("kql_MagicCodeAuthInput");

                    /* Select the text field */
                    copyText.select();

                    /* Copy the text inside the text field */
                    document.execCommand("copy");

                    /* Alert the copied text */
                    // alert("Copied the text: " + copyText.value);

                    var w = screen.width / 2;
                    var h = screen.height / 2;
                    params = 'width='+w+',height='+h
                    kql_MagicUserCodeAuthWindow = window.open('"""
                + url
                + """', 'kql_MagicUserCodeAuthWindow', params);

                    // TODO: save selected cell index, so that the clear will be done on the lince cell
                }
                </script>

                </body></html>"""
            )

            Display.show_html(html_str)
            # webbrowser.open(code['verification_url'])
            try:
                token = self._adal_context.acquire_token_with_device_code(self._kusto_cluster, code, self._client_id)
            finally:
                html_str = """<!DOCTYPE html>
                    <html><body><script>

                        // close authentication window
                        if (kql_MagicUserCodeAuthWindow && kql_MagicUserCodeAuthWindow.opener != null && !kql_MagicUserCodeAuthWindow.closed) {
                            kql_MagicUserCodeAuthWindow.close()
                        }
                        // TODO: make sure, you clear the right cell. BTW, not sure it is a must to do any clearing

                        // clear output cell
                        Jupyter.notebook.clear_output(Jupyter.notebook.get_selected_index())

                        // TODO: if in run all mode, move to last cell, otherwise move to next cell
                        // move to next cell

                    </script></body></html>"""

                Display.show_html(html_str)
        elif self._authentication_method is AuthenticationMethod.aad_application_certificate:
            token = self._adal_context.acquire_token_with_client_certificate(
                self._kusto_cluster, self._client_id, self._certificate, self._thumbprint
            )
        else:
            raise KustoClientError("Please choose authentication method from azure.kusto.data.security.AuthenticationMethod")
        return self._get_header(token)

    def _get_header(self, token):
        return "{0} {1}".format(token[TokenResponseFields.TOKEN_TYPE], token[TokenResponseFields.ACCESS_TOKEN])

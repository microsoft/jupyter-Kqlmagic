# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

"""A module to acquire tokens from AAD.
"""

import os
from enum import Enum, unique
from datetime import timedelta, datetime
import smtplib, ssl

from six.moves.urllib.parse import urlparse

import dateutil.parser
from adal import AuthenticationContext
from adal.constants import TokenResponseFields, OAuth2DeviceCodeResponseParameters
from Kqlmagic.constants import Constants
from Kqlmagic.log import logger
from Kqlmagic.display import Display
from Kqlmagic.constants import ConnStrKeys
from Kqlmagic.adal_token_cache import AdalTokenCache


class AuthenticationError(Exception):
    pass


class ConnKeysKCSB(object):
    """
    Object like dict, every dict[key] can be visited by dict.key
    """

    def __init__(self, conn_kv, data_source):
        self.conn_kv = conn_kv
        self.data_source = data_source
        self.translate_map = {
            "authority_id": ConnStrKeys.TENANT,
            "aad_user_id": ConnStrKeys.USERNAME,
            "password": ConnStrKeys.PASSWORD,
            "application_client_id": ConnStrKeys.CLIENTID,
            "application_key": ConnStrKeys.CLIENTSECRET,
            "application_certificate": ConnStrKeys.CERTIFICATE,
            "application_certificate_thumbprint": ConnStrKeys.CERTIFICATE_THUMBPRINT,
        }

    def __getattr__(self, kcsb_attr_name):
        if kcsb_attr_name == "data_source":
            return self.data_source
        key = self.translate_map.get(kcsb_attr_name)
        return self.conn_kv.get(key)


@unique
class AuthenticationMethod(Enum):
    """Enum represnting all authentication methods available in Azure Monitor with Python."""

    aad_username_password = "aad_username_password"
    aad_application_key = "aad_application_key"
    aad_application_certificate = "aad_application_certificate"
    aad_device_login = "aad_device_login"


_CLOUD_AAD_URLS={
        "public": "https://login.microsoftonline.com",
        "mooncake": "https://login.partner.microsoftonline.cn",
        "mooncake": "https://login.partner.microsoftonline.cn",
        "fairfax": "https://login.microsoftonline.us",
        "blackforest": "https://login.microsoftonline.de",
}


class _MyAadHelper(object):
    def __init__(self, kcsb, default_clientid, cloud):
        cloud = cloud or "public"
        if cloud.find("://") >= 0:
            cloud_url = cloud
        else:
            cloud_url = _CLOUD_AAD_URLS.get(cloud)


        authority = kcsb.authority_id or "common"
        client_id = kcsb.application_client_id or default_clientid
        self._resource = "{0.scheme}://{0.hostname}".format(urlparse(kcsb.data_source))
        token_cache = None
        isSso = "FALSE" # os.getenv("{0}_ENABLE_SSO".format(Constants.MAGIC_CLASS_NAME.upper()))
        if (isSso and isSso.upper() == "TRUE"):
            token_cache = AdalTokenCache()
        self._adal_context = AuthenticationContext("{0}/{1}".format(cloud_url, authority), cache=token_cache)
        self._username = None
        if all([kcsb.aad_user_id, kcsb.password]):
            self._authentication_method = AuthenticationMethod.aad_username_password
            self._client_id = client_id
            self._username = kcsb.aad_user_id
            self._password = kcsb.password
        elif all([kcsb.application_client_id, kcsb.application_key]):
            self._authentication_method = AuthenticationMethod.aad_application_key
            self._client_id = client_id
            self._client_secret = kcsb.application_key
        elif all([kcsb.application_client_id, kcsb.application_certificate, kcsb.application_certificate_thumbprint]):
            self._authentication_method = AuthenticationMethod.aad_application_certificate
            self._client_id = client_id
            self._certificate = kcsb.application_certificate
            self._thumbprint = kcsb.application_certificate_thumbprint
        else:
            self._authentication_method = AuthenticationMethod.aad_device_login
            self._client_id = client_id

    def acquire_token(self, **options):
        """Acquire tokens from AAD."""
        token = self._adal_context.acquire_token(self._resource, self._username, self._client_id)
        if token is not None:
            expiration_date = dateutil.parser.parse(token[TokenResponseFields.EXPIRES_ON])
            if expiration_date > datetime.now() + timedelta(minutes=1):
                logger().debug("_MyAadHelper::acquire_token - from Cache - resource: '%s', username: '%s', client: '%s'", self._resource, self._username, self._client_id)
                return self._get_header(token)
            if TokenResponseFields.REFRESH_TOKEN in token:
                token = self._adal_context.acquire_token_with_refresh_token(token[TokenResponseFields.REFRESH_TOKEN], self._client_id, self._resource)
                if token is not None:
                    logger().debug("_MyAadHelper::acquire_token - aad refresh - resource: '%s', username: '%s', client: '%s'", self._resource, self._username, self._client_id)
                    return self._get_header(token)

        if self._authentication_method is AuthenticationMethod.aad_username_password:
            logger().debug("_MyAadHelper::acquire_token - aad/user-password - resource: '%s', username: '%s', password: '...', client: '%s'", self._resource, self._username, self._client_id)
            token = self._adal_context.acquire_token_with_username_password(self._resource, self._username, self._password, self._client_id)
        elif self._authentication_method is AuthenticationMethod.aad_application_key:
            logger().debug("_MyAadHelper::acquire_token - aad/client-secret - resource: '%s', client: '%s', secret: '...'", self._resource, self._client_id)
            token = self._adal_context.acquire_token_with_client_credentials(self._resource, self._client_id, self._client_secret)
        elif self._authentication_method is AuthenticationMethod.aad_device_login:
            # print(code[OAuth2DeviceCodeResponseParameters.MESSAGE])
            # webbrowser.open(code[OAuth2DeviceCodeResponseParameters.VERIFICATION_URL])
            # token = self._adal_context.acquire_token_with_device_code(
            #     self._resource, code, self._client_id
            # )
            logger().debug("_MyAadHelper::acquire_token - aad/code - resource: '%s', client: '%s'", self._resource, self._client_id)
            code: dict = self._adal_context.acquire_user_code(self._resource, self._client_id)
            url = code[OAuth2DeviceCodeResponseParameters.VERIFICATION_URL]
            device_code = code[OAuth2DeviceCodeResponseParameters.USER_CODE].strip()
            

            if options.get("login_code_destination") !="browser" or  options.get("notebook_app")=="papermill":
                email_message = "Copy code: "+ device_code + " and authenticate in: " + url
                self.send_email(email_message, options.get("login_code_destination"))
               
            else:
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

                if options.get("notebook_app") in ["visualstudiocode", "ipython"]:
                    Display.show_window('verification_url', url, **options)
                    # Display.showInfoMessage("Code: {0}".format(device_code))
                    Display.showInfoMessage("Copy code: {0} to verification url: {1} and authenticate".format(device_code, url), **options)
                else:
                    Display.show_html(html_str)

            try:
                token = self._adal_context.acquire_token_with_device_code(self._resource, code, self._client_id)
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
            logger().debug("_MyAadHelper::acquire_token - aad/client-certificate - resource: '%s', client: '%s', _certificate: '...', thumbprint: '%s'", self._resource, self._client_id, self._thumbprint)
            token = self._adal_context.acquire_token_with_client_certificate(self._resource, self._client_id, self._certificate, self._thumbprint)
        else:
            raise AuthenticationError("Unknown authentication method.")
        return self._get_header(token)


    def send_email(self, message, mailto):

        port = 465  # For SSL
        smtp_server = "smtp.gmail.com"
        sender_email = "dev.kql.test@gmail.com"  # Enter your address

        receiver_email = mailto # Enter receiver address

        password = "Kc0qpELOz8V3"

        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
            server.login(sender_email, password)
            server.sendmail(sender_email, receiver_email, "\n"+message)

    def _get_header(self, token):
        return "{0} {1}".format(token[TokenResponseFields.TOKEN_TYPE], token[TokenResponseFields.ACCESS_TOKEN])

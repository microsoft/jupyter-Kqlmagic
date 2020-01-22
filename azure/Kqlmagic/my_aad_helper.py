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
from urllib.parse import urlparse
import uuid
import smtplib
import webbrowser


import dateutil.parser
from adal import AuthenticationContext
from adal.constants import TokenResponseFields, OAuth2DeviceCodeResponseParameters
import jwt


from .constants import Constants, Cloud
from .log import logger
from .display import Display
from .constants import ConnStrKeys
from .adal_token_cache import AdalTokenCache
from .kql_engine import KqlEngineError
from .parser import Parser
from .email_notification import EmailNotification


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
            "authority_id":                       ConnStrKeys.TENANT,
            "aad_url":                            ConnStrKeys.AAD_URL,
            "aad_user_id":                        ConnStrKeys.USERNAME,
            "password":                           ConnStrKeys.PASSWORD,
            "application_client_id":              ConnStrKeys.CLIENTID,
            "application_key":                    ConnStrKeys.CLIENTSECRET,
            "application_certificate":            ConnStrKeys.CERTIFICATE,
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


_CLOUD_AAD_URLS = {
        Cloud.PUBLIC :     "https://login.microsoftonline.com",
        Cloud.MOONCAKE:    "https://login.partner.microsoftonline.cn", # === 'login.chinacloudapi.cn'?
        Cloud.FAIRFAX:     "https://login.microsoftonline.us",
        Cloud.BLACKFOREST: "https://login.microsoftonline.de",
}


_CLOUD_DSTS_AAD_DOMAINS = {
        # Define dSTS domains whitelist based on its Supported Environments & National Clouds list here
        # https://microsoft.sharepoint.com/teams/AzureSecurityCompliance/Security/SitePages/dSTS%20Fundamentals.aspx
        Cloud.PUBLIC :      'dsts.core.windows.net',
        Cloud.MOONCAKE:     'dsts.core.chinacloudapi.cn',  
        Cloud.BLACKFOREST:  'dsts.core.cloudapi.de', 
        Cloud.FAIRFAX:      'dsts.core.usgovcloudapi.net'
}


# not cached shared context per authority
global_adal_context = {}

# cached shared context per authority
global_adal_context_sso = {}


class _MyAadHelper(object):

    def __init__(self, kcsb, default_clientid, adal_context = None, adal_context_sso = None, **options):
        global global_adal_context
        global global_adal_context_sso

        client_id = kcsb.application_client_id or default_clientid
        url = urlparse(kcsb.data_source)
        self._resource = f"{url.scheme}://{url.hostname}"
        self.sso_enabled = False

        cloud = options.get("cloud")
        if kcsb.conn_kv.get(ConnStrKeys.AAD_URL):
            aad_login_url = kcsb.conn_kv.get(ConnStrKeys.AAD_URL)
        else:
            aad_login_url = _CLOUD_AAD_URLS.get(cloud)

            if not aad_login_url:
                raise KqlEngineError(f"AAD is not known for this cloud {cloud}, please use aadurl property in connection string.")

        authority = kcsb.authority_id or "common"

        authority_key= f"{aad_login_url}/{authority}"

        self._adal_context = adal_context
        if self._adal_context is None:
            if not global_adal_context.get(authority_key):
                global_adal_context[authority_key] = AuthenticationContext(authority_key, cache=None)
            self._adal_context = global_adal_context.get(authority_key)

        self._adal_context_sso = None
        if options.get("enable_sso"):
            self._adal_context_sso = adal_context_sso
            if self._adal_context_sso is None:
                if not global_adal_context_sso.get(authority_key):
                    cache = AdalTokenCache.get_cache(authority_key, **options)
                    if cache:
                        global_adal_context_sso[authority_key] = AuthenticationContext(authority_key, cache=cache)
                self._adal_context_sso = global_adal_context_sso.get(authority_key)

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
            self._username = kcsb.aad_user_id # optional


    def acquire_token(self, **options):
        """Acquire tokens from AAD."""

        token = None
        if self._adal_context_sso:
            adal_context = self._adal_context_sso
            token = adal_context.acquire_token(self._resource, self._username, self._client_id)
            if not token:
                token = self._adal_context.acquire_token(self._resource, self._username, self._client_id)
                if token:
                    adal_context = self._adal_context
        else:
            adal_context = self._adal_context
            token = adal_context.acquire_token(self._resource, self._username, self._client_id)

        if token is not None:
            self._username = self._username or self._get_username_from_token(token)
            expiration_date = dateutil.parser.parse(token[TokenResponseFields.EXPIRES_ON])
            if expiration_date > datetime.now() + timedelta(minutes=1):
                logger().debug("_MyAadHelper::acquire_token - from Cache - resource: '%s', username: '%s', client: '%s'", self._resource, self._username, self._client_id)
                return self._get_header(token)
            if TokenResponseFields.REFRESH_TOKEN in token:
                token = adal_context.acquire_token_with_refresh_token(token[TokenResponseFields.REFRESH_TOKEN], self._client_id, self._resource)
                if token is not None:
                    logger().debug("_MyAadHelper::acquire_token - aad refresh - resource: '%s', username: '%s', client: '%s'", self._resource, self._username, self._client_id)
                    return self._get_header(token)

        if self._authentication_method is AuthenticationMethod.aad_username_password:
            logger().debug("_MyAadHelper::acquire_token - aad/user-password - resource: '%s', username: '%s', password: '...', client: '%s'", self._resource, self._username, self._client_id)
            token = adal_context.acquire_token_with_username_password(self._resource, self._username, self._password, self._client_id)

        elif self._authentication_method is AuthenticationMethod.aad_application_key:
            logger().debug("_MyAadHelper::acquire_token - aad/client-secret - resource: '%s', client: '%s', secret: '...'", self._resource, self._client_id)
            token = adal_context.acquire_token_with_client_credentials(self._resource, self._client_id, self._client_secret)

        elif self._authentication_method is AuthenticationMethod.aad_device_login:
            logger().debug("_MyAadHelper::acquire_token - aad/code - resource: '%s', client: '%s'", self._resource, self._client_id)
            code: dict = adal_context.acquire_user_code(self._resource, self._client_id)
            url = code[OAuth2DeviceCodeResponseParameters.VERIFICATION_URL]
            device_code = code[OAuth2DeviceCodeResponseParameters.USER_CODE].strip()
            
            # if  options.get("notebook_app")=="papermill" and options.get("login_code_destination") =="browser":
            #     raise Exception("error: using papermill without an email specified is not supported")
            if options.get("device_code_login_notification") =="email":
                params = Parser.parse_and_get_kv_string(options.get('device_code_notification_email'), {})
                email_notification = EmailNotification(**params)
                subject = f"Kqlmagic device_code {device_code} authentication (context: {email_notification.context})"
                resource = self._resource.replace("://", ":// ") # just to make sure it won't be replace in email by safelinks
                email_message = f"Device_code: {device_code}\n\nYou are asked to authorize access to resource: {resource}\n\nOpen the page {url} and enter the code {device_code} to authenticate\n\nKqlmagic"
                email_notification.send_email(subject, email_message)
                Display.showInfoMessage(f"An email was sent to {email_notification.send_to} with device_code {device_code} to authenticate", **options)

               
            elif options.get("device_code_login_notification") =="browser":
                resource = self._resource
                self._open_redirection_page(resource, url, device_code)


            elif options.get("device_code_login_notification") =="terminal":
                print(code[OAuth2DeviceCodeResponseParameters.MESSAGE])

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
                    Display.showInfoMessage(f"Copy code: {device_code} to verification url: {url} and authenticate", **options)
                else:
                    Display.show_html(html_str)

            try:
                token = adal_context.acquire_token_with_device_code(self._resource, code, self._client_id)
                # logger().debug(f"_MyAadHelper::acquire_token - {token}")
                # TODO: what we should do if they are not the same???
                self._username = self._username or self._get_username_from_token(token)

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
            token = adal_context.acquire_token_with_client_certificate(self._resource, self._client_id, self._certificate, self._thumbprint)
        else:
            raise AuthenticationError("Unknown authentication method.")
        return self._get_header(token)


    def _open_redirection_page(self, resource, login_url, device_code):
        import tempfile
        from .my_utils import adjust_path
        from .help import MarkdownString

        before = MarkdownString(f""" You are asked to authorize to gain access to resource: {resource} """)._repr_html_()
        href = """Open the page  <a href=" """ +login_url + """ ">""" + login_url + """ </a>"""
        after = MarkdownString(f""" and enter the code {device_code} to authenticate. \n KqlMagic.""")._repr_html_()
        html = str(before) + str(href) + str(after)
        path = Display._html_to_file_path(html, "login_device_code")
        full_path = Display.showfiles_base_path + "/" + adjust_path(path)
        url = 'file://' + full_path
        webbrowser.open(url)


    def _get_header(self, token):
        return f"{token[TokenResponseFields.TOKEN_TYPE]} {token[TokenResponseFields.ACCESS_TOKEN]}"


    def _get_username_from_token(self, token):
        claims = jwt.decode(token.get('accessToken'), verify=False)
        username = claims.get('upn') or claims.get('email') or claims.get('sub')
        return username


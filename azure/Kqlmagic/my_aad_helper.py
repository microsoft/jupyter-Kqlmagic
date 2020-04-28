# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

"""A module to acquire tokens from AAD.
"""

import os
import time
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


# shared not cached context per authority
global_adal_context = {}

# shared cached context per authority
global_adal_context_sso = {}

class OAuth2TokenFields(object):
    # taken from here: https://docs.microsoft.com/en-us/azure/app-service/overview-managed-identity?tabs=dotnet

    # The requested access token. The calling web service can use this token to authenticate to the receiving web service.
    ACCESS_TOKEN = 'access_token'

    # The client ID of the identity that was used.
    CLIENT_ID = 'client_id'

    # The timespan when the access token expires. The date is represented as the number of seconds from "1970-01-01T0:0:0Z UTC" (corresponds to the token's exp claim).
    EXPIRES_ON = 'expires_on'

    # The timespan when the access token takes effect, and can be accepted. The date is represented as the number of seconds from "1970-01-01T0:0:0Z UTC" (corresponds to the token's nbf claim).
    NOT_BEFORE = 'not_before'

    # The resource the access token was requested for, which matches the resource query string parameter of the request.
    RESOURCE = 'resource'

    # Indicates the token type value. The only type that Azure AD supports is FBearer. For more information about bearer tokens, see The OAuth 2.0 Authorization Framework: Bearer Token Usage (RFC 6750).
    TOKEN_TYPE = 'token_type'

    # optional
    ID_TOKEN = 'id_token'
    REFRESH_TOKEN = 'refresh_token'


class _MyAadHelper(object):

    def __init__(self, kcsb, default_clientid, adal_context = None, adal_context_sso = None, **options):
        global global_adal_context
        global global_adal_context_sso

        # to provide stickiness, to avoid switching tokens when not required
        self._current_token = None
        self._current_adal_context = None
        self._auth_method_name = None

        # options are freezed for authentication when object is created, 
        # to eliminate the need to specify auth option on each query, and to modify behavior on exah query
        self._options = {**options}

        # track warning to avoid repeating
        self._displayed_warnings = []

        url = urlparse(kcsb.data_source)
        self._resource = f"{url.scheme}://{url.hostname}"

        self._authority = kcsb.authority_id  or "common"

        self._aad_login_url = self._get_aad_login_url(kcsb.conn_kv.get(ConnStrKeys.AAD_URL))

        self._set_adal_context(adal_context=adal_context, adal_context_sso=adal_context_sso)


        self._client_id = kcsb.application_client_id or default_clientid

        self._username = None
        if all([kcsb.aad_user_id, kcsb.password]):
            self._authentication_method = AuthenticationMethod.aad_username_password
            self._username = kcsb.aad_user_id
            self._password = kcsb.password
        elif all([kcsb.application_client_id, kcsb.application_key]):
            self._authentication_method = AuthenticationMethod.aad_application_key
            self._client_secret = kcsb.application_key
        elif all([kcsb.application_client_id, kcsb.application_certificate, kcsb.application_certificate_thumbprint]):
            self._authentication_method = AuthenticationMethod.aad_application_certificate
            self._certificate = kcsb.application_certificate
            self._thumbprint = kcsb.application_certificate_thumbprint
        else:
            self._authentication_method = AuthenticationMethod.aad_device_login
            self._username = kcsb.aad_user_id # optional


    def acquire_token(self):
        """Acquire tokens from AAD."""
        adal_context = None
        token = None

        if token is None:
            if self._current_token is not None:
                token = self._current_token
                token = self._get_fresh_enough_token(token, self._current_adal_context, self._auth_method_name)

        if token is None:
            if self._options.get("try_token") is not None:
                token = self._options.get("try_token")
                token = self._get_fresh_enough_token(token, adal_context, "try_token")

        if token is None:
            if self._options.get("try_msi") is not None:
                token = self._get_msi_token(msi_params=self._options.get("try_msi"))
                token = self._get_fresh_enough_token(token, adal_context, "try_msi")

        if token is None:
            if self._options.get("try_azcli_login_subscription") is not None:
                token = self._get_azcli_token(subscription=self._options.get("try_azcli_login_subscription"))
                token = self._get_fresh_enough_token(token, adal_context, "try_azcli_login_subscription")

        if token is None:
            if self._options.get("try_azcli_login"):
                token = self._get_azcli_token()
                token = self._get_fresh_enough_token(token, adal_context, "try_azcli_login")

        if token is None:
            if self._adal_context_sso:
                adal_context = self._adal_context_sso
                token = adal_context.acquire_token(self._resource, self._username, self._client_id)
                if token is None:
                    token = self._adal_context.acquire_token(self._resource, self._username, self._client_id)
                    if token is not None:
                        adal_context = self._adal_context
            else:
                adal_context = self._adal_context
                token = adal_context.acquire_token(self._resource, self._username, self._client_id)
            token = self._get_fresh_enough_token(token, adal_context, Constants.MAGIC_CLASS_NAME)

        if token is not None:
            logger().debug(f"_MyAadHelper::acquire_token - valid token exist - resource: '{self._resource}', username: '{self._username}', client: '{self._client_id}'")
            token = token

        elif self._authentication_method is AuthenticationMethod.aad_username_password:
            logger().debug(f"_MyAadHelper::acquire_token - aad/user-password - resource: '{self._resource}', username: '{self._username}', password: '...', client: '{self._client_id}'")
            token = adal_context.acquire_token_with_username_password(self._resource, self._username, self._password, self._client_id)

        elif self._authentication_method is AuthenticationMethod.aad_application_key:
            logger().debug(f"_MyAadHelper::acquire_token - aad/client-secret - resource: '{self._resource}', client: '{self._client_id}', secret: '...'")
            token = adal_context.acquire_token_with_client_credentials(self._resource, self._client_id, self._client_secret)

        elif self._authentication_method is AuthenticationMethod.aad_device_login:
            logger().debug(f"_MyAadHelper::acquire_token - aad/code - resource: '{self._resource}', client: '{self._client_id}'")
            code: dict = adal_context.acquire_user_code(self._resource, self._client_id)
            url = code[OAuth2DeviceCodeResponseParameters.VERIFICATION_URL]
            device_code = code[OAuth2DeviceCodeResponseParameters.USER_CODE].strip()

            device_code_login_notification = self._options.get("device_code_login_notification")
            if device_code_login_notification == "auto":
                if self._options.get("notebook_app") in ["ipython"]:
                    device_code_login_notification = "popup_interaction"
                elif self._options.get("notebook_app") in ["visualstudiocode", "azuredatastudio"]:
                    device_code_login_notification = "popup_interaction"
                elif self._options.get("notebook_app") in ["nteract"]:

                    if self._options.get("kernel_location") == "local":
                        # ntreact cannot execute authentication script, workaround using temp_file_server webbrowser
                        if self._options.get("temp_files_server_address") is not None:
                            import urllib.parse
                            indirect_url = f'{self._options.get("temp_files_server_address")}/webbrowser?url={urllib.parse.quote(url)}&kernelid={self._options.get("kernel_id")}'
                            url = indirect_url
                            device_code_login_notification = "popup_interaction"
                        else:
                            device_code_login_notification = "browser"
                    else:
                        device_code_login_notification = "terminal"
                else:
                    device_code_login_notification = "button"

            if (self._options.get("kernel_location") == "local" or 
                device_code_login_notification in ["browser"] or 
                (device_code_login_notification == "popup_interaction" and self._options.get("popup_interaction") == "webbrowser_open_at_kernel")):
                # copy code to local clipboard
                import pyperclip
                pyperclip.copy(device_code)

            # if  self._options.get("notebook_app")=="papermill" and self._options.get("login_code_destination") =="browser":
            #     raise Exception("error: using papermill without an email specified is not supported")
            if device_code_login_notification == "email":
                params = Parser.parse_and_get_kv_string(self._options.get('device_code_notification_email'), {})
                email_notification = EmailNotification(**params)
                subject = f"Kqlmagic device_code {device_code} authentication (context: {email_notification.context})"
                resource = self._resource.replace("://", ":// ") # just to make sure it won't be replace in email by safelinks
                email_message = f"Device_code: {device_code}\n\nYou are asked to authorize access to resource: {resource}\n\nOpen the page {url} and enter the code {device_code} to authenticate\n\nKqlmagic"
                email_notification.send_email(subject, email_message)
                info_message =f"An email was sent to {email_notification.send_to} with device_code {device_code} to authenticate"
                Display.showInfoMessage(info_message, display_handler_name='acquire_token', **self._options)

            elif device_code_login_notification == "browser":
                # this print is not for debug
                print(code[OAuth2DeviceCodeResponseParameters.MESSAGE])
                webbrowser.open(code[OAuth2DeviceCodeResponseParameters.VERIFICATION_URL])

            elif device_code_login_notification == "terminal":
                # this print is not for debug
                print(code[OAuth2DeviceCodeResponseParameters.MESSAGE])

            elif device_code_login_notification == "popup_interaction":
                before_text = f"<b>{device_code}</b>"
                button_text = "Copy code to clipboard and authenticate"
                # before_text = f"Copy code: {device_code} to verification url: {url} and "
                # button_text='authenticate'
                # Display.showInfoMessage(f"Copy code: {device_code} to verification url: {url} and authenticate", display_handler_name='acquire_token', **options)
                Display.show_window(
                    'verification_url',
                    url,
                    button_text=button_text,
                    # palette=Display.info_style,
                    before_text=before_text,
                    display_handler_name='acquire_token',
                    **self._options
                )

            else: # device_code_login_notification == "button":
                html_str = (
                    f"""<!DOCTYPE html>
                    <html><body>

                    <!-- h1 id="user_code_p"><b>{device_code}</b><br></h1-->

                    <input  id="kql_MagicCodeAuthInput" type="text" readonly style="font-weight: bold; border: none;" size = '{str(len(device_code))}' value='{device_code}'>

                    <button id='kql_MagicCodeAuth_button', onclick="this.style.visibility='hidden';kql_MagicCodeAuthFunction()">Copy code to clipboard and authenticate</button>

                    <script>
                    var kql_MagicUserCodeAuthWindow = null;
                    function kql_MagicCodeAuthFunction() {{
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
                        kql_MagicUserCodeAuthWindow = window.open('{url}', 'kql_MagicUserCodeAuthWindow', params);

                        // TODO: save selected cell index, so that the clear will be done on the lince cell
                    }}
                    </script>

                    </body></html>"""
                )
                Display.show_html(html_str, display_handler_name='acquire_token', **self._options)

            try:
                token = adal_context.acquire_token_with_device_code(self._resource, code, self._client_id)
                self._username = self._username or token.get(TokenResponseFields.USER_ID)

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

                Display.show_html(html_str, display_handler_name='acquire_token', **self._options)
        elif self._authentication_method is AuthenticationMethod.aad_application_certificate:
            logger().debug(f"_MyAadHelper::acquire_token - aad/client-certificate - resource: '{self._resource}', client: '{self._client_id}', _certificate: '...', thumbprint: '{self._thumbprint}'")
            token = adal_context.acquire_token_with_client_certificate(self._resource, self._client_id, self._certificate, self._thumbprint)
        else:
            raise AuthenticationError("Unknown authentication method.")

        if token is None:
            raise AuthenticationError("Failed to authenticate.")

        if self._current_token != token:
            self._warn_if_different(token)

        self._current_token = token
        self._current_adal_context = adal_context

        return self._get_header_from_token(token)


    # def email_format(self, dest):
    #     return re.match( r'[\w\.-]+@[\w\.-]+(\.[\w]+)+', dest)

    # def check_email_params(self, port, smtp_server, sender_email, receiver_email, password):
    #     if port and smtp_server and sender_email and receiver_email and password:
    #         if self.email_format(sender_email) and self.email_format(receiver_email):
    #             return True
    #     return False
    

    # def send_email(self, message, key_vals):

    #     port = key_vals.get("smtpport")  
    #     smtp_server = key_vals.get("smtpendpoint")
    #     sender_email = key_vals.get("sendfrom")

    #     receiver_email = key_vals.get("sendto") 

    #     password = key_vals.get("sendfrompassword")

    #     if not self.check_email_params(port,smtp_server, sender_email, receiver_email, password):
    #         raise ValueError("""
    #             cannot send login code to email because of missing or invalid environmental parameters. 
    #             Set KQLMAGIC_CODE_NOTIFICATION_EMAIL in the following way: SMTPEndPoint: \" email server\"; SMTPPort: \"email port\"; 
    #             sendFrom: \"sender email address \"; sendFromPassword: \"email address password \"; sendTo:\" email address to send to\"""" )

    #     # context = ssl.create_default_context()
    #     # with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:

    #     with smtplib.SMTP(smtp_server, port) as server:
    #         server.starttls() 
    #         server.login(sender_email, password)
    #         server.sendmail(sender_email, receiver_email, "\n"+message)

    #
    # Assume OAuth2 format (e.g. MSI Token) too
    #
    def _get_token_access_token(self, token:dict, default_access_token:str=None)->str:
        return token.get(TokenResponseFields.ACCESS_TOKEN) or token.get(OAuth2TokenFields.ACCESS_TOKEN) or default_access_token


    def _get_token_client_id(self, token:dict, default_client_id:str=None)->str:
        return token.get(TokenResponseFields._CLIENT_ID) or token.get(OAuth2TokenFields.CLIENT_ID) or default_client_id


    def _get_token_expires_on(self, token:dict, default_expires_on:str=None)->str:
        expires_on = default_expires_on
        if token.get(TokenResponseFields.EXPIRES_ON) is not None:
            expires_on = token.get(TokenResponseFields.EXPIRES_ON)
        elif token.get(OAuth2TokenFields.EXPIRES_ON) is not None:
            # The date is represented as the number of seconds from "1970-01-01T0:0:0Z UTC" (corresponds to the token's exp claim).
            expires_on = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(token.get(OAuth2TokenFields.EXPIRES_ON)))
        return expires_on


    def _get_token_not_before(self, token:dict, default_not_before:str=None)->str:
        not_before = default_not_before
        if token.get(OAuth2TokenFields.NOT_BEFORE) is not None:
            # The date is represented as the number of seconds from "1970-01-01T0:0:0Z UTC" (corresponds to the token's nbf claim).
            not_before = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(token.get(OAuth2TokenFields.NOT_BEFORE)))
        return not_before


    def _get_token_token_type(self, token:dict, default_token_type:str=None)->str:
        return token.get(TokenResponseFields.TOKEN_TYPE) or token.get(OAuth2TokenFields.TOKEN_TYPE) or default_token_type


    def _get_token_resource(self, token:dict, default_resource:str=None)->str:
        return token.get(TokenResponseFields.RESOURCE) or token.get(OAuth2TokenFields.RESOURCE) or default_resource


    def _get_token_user_id(self, token:dict, default_user_id:str=None)->str:
        return token.get(TokenResponseFields.USER_ID) or default_user_id


    def _get_token_refresh_token(self, token:dict, default_refresh_token:str=None)->str:
        return token.get(TokenResponseFields.REFRESH_TOKEN) or token.get(OAuth2TokenFields.REFRESH_TOKEN) or default_refresh_token


    def _get_token_id_token(self, token:dict, default_id_token:str=None)->str:
        return token.get(OAuth2TokenFields.ID_TOKEN) or default_id_token


    def _get_token_authority(self, token:dict, default_authority:str=None)->str:
        return token.get(TokenResponseFields._AUTHORITY ) or default_authority


    def _get_header_from_token(self, token:dict)->str:
        access_token = self._get_token_access_token(token)
        if access_token is None:
            raise AuthenticationError("Not a valid token, property 'access_token' is not present.")

        token_type = self._get_token_token_type(token)
        if token_type is None:
            raise AuthenticationError("Unable to determine the token type. Neither 'tokenType' nor 'token_type' property is present.")

        return self._get_header(token_type, access_token)


    def _get_header(self, token_type:str, access_token:str)->str:
        return f"{token_type} {access_token}"


    def _get_username_from_token(self, token:str)->str:
        claims = jwt.decode(self._get_token_id_token(token) or self._get_token_access_token(token), verify=False)
        username = claims.get("unique_name") or claims.get("upn") or claims.get("email") or claims.get("sub")
        return username


    def _get_expires_on_from_token(self, token:str)->str:
        expires_on = None
        claims = jwt.decode(self._get_token_access_token(token), verify=False)
        exp = claims.get("exp")
        if exp is not None:
            expires_on = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(exp))
        return expires_on


    def _get_not_before_from_token(self, token:str)->str:
        not_before = None
        claims = jwt.decode(self._get_token_access_token(token), verify=False)
        nbf = claims.get("nbf")
        if nbf is not None:
            not_before = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(nbf))
        return not_before


    def _get_client_id_from_token(self, token:str)->str:
        claims = jwt.decode(self._get_token_access_token(token), verify=False)
        client_id = claims.get("client_id") or claims.get("appid") or claims.get("azp")
        return client_id

    
    def _get_resources_from_token(self, token:str)->list:
        claims = jwt.decode(self._get_token_access_token(token), verify=False)
        resources = claims.get("aud")
        if type(resources) == str:
            resources = [resources]
        return resources


    def _get_authority_from_token(self, token:str)->str:
        claims = jwt.decode(self._get_token_access_token(token), verify=False)
        tenant_id = claims.get("tid")
        issuer = claims.get("iss")
        authority_uri = None

        if tenant_id is None and issuer is not None and issuer.startswith("http"):
            from urllib.parse import urlparse
            url_obj = urlparse(issuer)
            tenant_id = url_obj.path

        if tenant_id is not None:
            if tenant_id.startswith("http"):
                authority_uri = tenant_id
            else:
                if tenant_id.startswith("/"):
                    tenant_id = tenant_id[1:]
                if tenant_id.endswith("/"):
                    tenant_id = tenant_id[:-1]
                authority_uri = f"{self._aad_login_url}/{tenant_id}"

        return authority_uri


    def _get_azcli_token(self, subscription:str=None)->str:
        token = None
        try:
            # requires azure-cli-core to be installed
            # from azure.cli.core._profile import _CLIENT_ID as AZCLI_CLIENT_ID
            from azure.common.credentials import get_cli_profile 
            tenant = self._authority if subscription is None else None
            try:
                profile = get_cli_profile()
                credential, _subscription, _tenant = profile.get_raw_token(resource=self._resource, subscription=subscription, tenant=tenant)
                token_type, access_token, token = credential
            except:
                pass
        except [ImportError, ModuleNotFoundError]:
            raise AuthenticationError("Azure CLI authentication requires 'azure-cli-core' to be installed.")
        except:
            pass
        logger().debug(f"_MyAadHelper::_get_azcli_token {'failed' if token is None else 'succeeded'} to get token - subscription: '{subscription}', tenant: '{tenant}'")
        return token


    def _get_msi_token(self, msi_params={})->str:
        token = None
        try:
            from msrestazure.azure_active_directory import MSIAuthentication
            try:
                # allow msi_params to overrite the connection string resource
                credentials = MSIAuthentication(**{"resource":self._resource, **msi_params})
                token = credentials.token
            except:
                pass
        except [ImportError, ModuleNotFoundError]:
            raise AuthenticationError("MSI authentication requires 'msrestazure' to be installed.")
        except:
            pass
        logger().debug(f"_MyAadHelper::_get_msi_token {'failed' if token is None else 'succeeded'} to get token - msi_params: '{msi_params}'")
        return token


    def _get_fresh_enough_token(self, token:str, adal_context, auth_method_name:str)->str:
        fresh_enough_token = None
        if token is not None:
            self._auth_method_name = auth_method_name
            resource = self._get_token_resource(token) or self._resource

            not_before = self._get_token_not_before(token) or self._get_not_before_from_token(token)
            if not_before is not None:
                not_before_datetime = dateutil.parser.parse(not_before)
                current_datetime = datetime.now() - timedelta(minutes=1)
                if not_before_datetime > current_datetime:
                    logger().debug(f"_MyAadHelper::_get_fresh_enough_token - failed - token can be used not before {not_before} - resource: '{resource}'")
                    self._warn_on_refresh(f"access token cannot be used before {not_before}")
                    return None

            expires_on = self._get_token_expires_on(token) or self._get_expires_on_from_token(token)
            if expires_on is not None:
                expiration_datetime = dateutil.parser.parse(expires_on)
            else:
                expiration_datetime = datetime.now() + timedelta(minutes=30)

            current_datetime = datetime.now() + timedelta(minutes=1)
            if expiration_datetime > current_datetime:
                fresh_enough_token = token
                logger().debug(f"_MyAadHelper::_get_fresh_enough_token - succeeded, no need to refresh yet, expires on {expires_on} - resource: '{resource}'")
            else:
                logger().debug(f"_MyAadHelper::_get_fresh_enough_token - token expires on {expires_on} need to refresh - resource: '{resource}'")
                refresh_token = self._get_token_refresh_token(token)
                if refresh_token is not None:
                    try:
                        if adal_context is None:
                            authority_uri = self._get_token_authority(token) or self._get_authority_from_token(token) or self._authority_uri
                            adal_context = AuthenticationContext(authority_uri, cache=None)
                        client_id = self._get_token_client_id(token) or self._get_client_id_from_token(token) or self._client_id
                        fresh_enough_token = adal_context.acquire_token_with_refresh_token(refresh_token, client_id, resource)
                    except Exception as e:
                        self._warn_on_refresh(f"access token expired on {expires_on}, failed to refresh access token, Exception: {e}")

                    logger().debug(f"_MyAadHelper::_get_fresh_enough_token - {'failed' if token is None else 'succeeded'} to refresh token - resource: '{resource}'")
                else:
                    logger().debug(f"_MyAadHelper::_get_fresh_enough_token - failed to refresh expired token, token doesn't contain refresh token - resource: '{resource}'")
                    self._warn_on_refresh(f"access token expired on {expires_on}, and token entry has no refresh_token")

        return fresh_enough_token


    def _set_adal_context(self, adal_context=None, adal_context_sso=None):
        self._authority_uri = f"{self._aad_login_url}/{self._authority}"

        self._adal_context = adal_context
        if self._adal_context is None:
            if global_adal_context.get(self._authority_uri) is None:
                global_adal_context[self._authority_uri] = AuthenticationContext(self._authority_uri, cache=None)
            self._adal_context = global_adal_context.get(self._authority_uri)

        self._adal_context_sso = None
        if self._options.get("enable_sso"):
            self._adal_context_sso = adal_context_sso
            if self._adal_context_sso is None:
                if global_adal_context_sso.get(self._authority_uri) is None:
                    cache = AdalTokenCache.get_cache(self._authority_uri, **self._options)
                    if cache is not None:
                        global_adal_context_sso[self._authority_uri] = AuthenticationContext(self._authority_uri, cache=cache)
                self._adal_context_sso = global_adal_context_sso.get(self._authority_uri)


    def _get_aad_login_url(self, aad_login_url=None):
        if aad_login_url is None:
            cloud = self._options.get("cloud")
            aad_login_url = _CLOUD_AAD_URLS.get(cloud)
            if aad_login_url is None:
                raise KqlEngineError(f"AAD is not known for this cloud '{cloud}', please use aadurl property in connection string.")
        return aad_login_url


    def _warn_on_refresh(self, message)->None:
        if self._options.get("auth_token_warnings"):
            if self._auth_method_name is not None and message is not None:
                warn_message =f"Can't use '{self._auth_method_name}' token entry, {message}'"
                Display.showWarningMessage(warn_message, display_handler_name='acquire_token', **self._options)


    def _warn_if_different(self, token:str)->None:
        if self._options.get("auth_token_warnings"):
            if token is not None:
                # to avoid more than one warning per connection, keep track of already displayed warnings
                access_token = self._get_token_access_token(token)
                key = hash((access_token))
                if key in self._displayed_warnings:
                    return
                else:
                    self._displayed_warnings.append(key)

                token_username = self._get_token_user_id(token) or self._get_username_from_token(token)
                if token_username is not None and self._username is not None and token_username != self._username:
                    warn_message =f"authenticated username '{token_username}' is different from connectiion string username '{self._username}'"
                    Display.showWarningMessage(warn_message, display_handler_name='acquire_token', **self._options)

                token_authority_uri = self._get_token_authority(token) or self._get_authority_from_token(token)
                if token_authority_uri != self._authority_uri and not self._authority_uri.endswith("/common") and not token_authority_uri.endswith("/common"):
                    warn_message =f"authenticated authority '{token_authority_uri}' is different from connectiion string authority '{self._authority_uri}'"
                    Display.showWarningMessage(warn_message, display_handler_name='acquire_token', **self._options)

                token_client_id = self._get_token_client_id(token) or self._get_client_id_from_token(token)
                if token_client_id is not None and self._client_id is not None and token_client_id != self._client_id:
                    warn_message =f"authenticated client_id '{token_client_id}' is different from connectiion string client_id '{self._client_id}'"
                    Display.showWarningMessage(warn_message, display_handler_name='acquire_token', **self._options)

                token_resources = self._get_token_resource(token) or self._get_resources_from_token(token)
                if type(token_resources) == str:
                    token_resources = [token_resources]
                if token_resources is not None and self._resource is not None and self._resource not in token_resources:
                    warn_message =f"authenticated resources '{token_resources}' does not include connectiion string resource '{self._resource}'"
                    Display.showWarningMessage(warn_message, display_handler_name='acquire_token', **self._options)

# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

"""A module to acquire tokens from AAD.
"""

import os
import sys
from io import StringIO
import time
from datetime import timedelta, datetime
from urllib.parse import urlparse
# import webbrowser
import json
from base64 import urlsafe_b64decode


import dateutil.parser

from .dependencies import Dependencies
from .my_utils import single_quote
from .constants import Cloud
from .log import logger
from .display import Display
from .constants import ConnStrKeys
from .msal_token_cache import MsalTokenCache 
from .exceptions import KqlEngineError
from .parser import Parser
from .email_notification import EmailNotification
from .aad_helper import AadHelper
from .os_dependent_api import OsDependentAPI


class OAuth2DeviceCodeResponseParameters(object):
    USER_CODE         = 'user_code'
    DEVICE_CODE       = 'device_code'
    VERIFICATION_URL  = 'verification_uri'
    EXPIRES_IN        = 'expires_in'
    EXPIRES_AT        = 'expires_at'
    CORRELATION_ID    = '_correlation_id'
    INTERVAL          = 'interval'
    MESSAGE           = 'message'
    ERROR             = 'error'
    ERROR_DESCRIPTION = 'error_description'

# tokens from adal based authentication (current azcli, msi are using adal) user-token may be with this format too


class TokenResponseFieldsV1(object):
    TOKEN_TYPE = 'tokenType'
    ACCESS_TOKEN = 'accessToken'
    REFRESH_TOKEN = 'refreshToken'
    CREATED_ON = 'createdOn'
    EXPIRES_ON = 'expiresOn'
    EXPIRES_IN = 'expiresIn'
    RESOURCE = 'resource'
    USER_ID = 'userId'
    
    # not from the wire, but amends for token cache
    _AUTHORITY = '_authority'
    _CLIENT_ID = '_clientId'
    IS_MRRT = 'isMRRT'
        
    ERROR = 'error'
    ERROR_DESCRIPTION = 'errorDescription'


# tokens created using AAD graph or msal library
class TokenResponseFieldsV2(object):
    TOKEN_TYPE = 'token_type'
    SCOPE = 'scope'  # '<scope-explicit> <SPACE> <scope-request>'  (resource with role)
    EXPIRES_IN = 'expires_in'
    EXT_EXPIRES_IN = 'ext_expires_in'
    ACCESS_TOKEN = 'access_token'
    REFRESH_TOKEN = 'refresh_token'
    ID_TOKEN = 'id_token'

    CLIENT_INFO = 'client_info'  # base64: {"uid":"<oid>","utid":"<tid>"}
    ID_TOKEN_CLAIMS = 'id_token_claims'

    # error response
    ERROR = 'error'
    ERROR_DESCRIPTION = 'error_description'
    ERROR_CODES = 'error_codes'  # array
    TIMESTAMP = 'timestamp'
    TRACE_ID = 'trace_id'
    CORRELATION_ID = 'correlation_id'
    ERROR_URI = 'error_uri'


class IdTokenClaims(object):
    ISS = 'iss'  # authority
    AUD = 'aud'  # resource
    PREFERRED_USERNAME = 'preferred_username'  # user_id


# tokens created managed identity REST API
class OAuth2TokenFields(object):
    # taken from here: https://docs.microsoft.com/en-us/azure/app-service/overview-managed-identity?tabs=dotnet

    # The requested access token. The calling web service can use this token to authenticate to the receiving web service.
    ACCESS_TOKEN = 'access_token'

    # The client ID of the identity that was used.
    CLIENT_ID = 'client_id'  # claims appid

    # The timespan when the access token expires. The date is represented as the number of seconds from "1970-01-01T0:0:0Z UTC" (corresponds to the token's exp claim).
    EXPIRES_ON = 'expires_on'  # claims exp, seconds from 1970

    # The timespan when the access token takes effect, and can be accepted.
    # The date is represented as the number of seconds from "1970-01-01T0:0:0Z UTC" (corresponds to the token's nbf claim).
    NOT_BEFORE = 'not_before'  # claims nbf, seconds from 1970

    # The resource the access token was requested for, which matches the resource query string parameter of the request.
    RESOURCE = 'resource'  # claims aud

    # Indicates the token type value. The only type that Azure AD supports is FBearer. For more information about bearer tokens, 
    # see The OAuth 2.0 Authorization Framework: Bearer Token Usage (RFC 6750).
    TOKEN_TYPE = 'token_type'  # 'bearer

    # optional
    ID_TOKEN = 'id_token'
    REFRESH_TOKEN = 'refresh_token'


class AuthenticationError(Exception):
    """Raised when authentication fails."""

    def __init__(self, exception, acquire_token_result=None, **kwargs):
        super(AuthenticationError, self).__init__()
        exception = exception.exception if isinstance(exception, AuthenticationError) else exception
        self.authentication_method = kwargs.get("authentication_method")
        self.authority = kwargs.get("authority")
        self.resource = kwargs.get("resource")
        self.exception = exception
        self.kwargs = kwargs

        if acquire_token_result is not None:
            self.exception = f'{self.exception}, details: {acquire_token_result}'

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return f"AuthenticationError('{self.authentication_method}', '{repr(self.exception)}', '{self.kwargs}')"


class ConnKeysKCSB(object):
    """
    Object like dict, every dict[key] can be visited by dict.key
    """

    def __init__(self, conn_kv, data_source):
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
        
        self.kcsb = {k: conn_kv.get(self.translate_map.get(k), None) for k in self.translate_map}
        self.kcsb["data_source"] = data_source

        self._json_dumps = json.dumps(self.kcsb, sort_keys=True)
        self._hash = hash(self._json_dumps)


    def __getattr__(self, kcsb_attr_name):
        return self.kcsb.get(kcsb_attr_name, None)


    def __eq__(self, other:any)-> bool:
        return isinstance(other, ConnKeysKCSB) and self._json_dumps == other._json_dumps

    
    def __hash__(self):
        return self._hash


    def __str__(self)->str:
        return self._json_dumps


class ClientAppType(object):
    public       = "public"
    confidential = "confidential"


class AuthenticationMethod(object):
    """Represnting all authentication methods available in Azure Monitor with Python."""

    aad_username_password       = "aad_username_password"
    aad_application_key         = "aad_application_key"
    aad_application_certificate = "aad_application_certificate"
    aad_code_login              = "aad_code_login"

    # external tokens
    azcli_login                 = "azcli_login"
    azcli_login_subscription    = "azcli_login_subscription"
    azcli_login_by_profile      = "azcli_login_by_profile"
    managed_service_identity    = "managed_service_identity"
    vscode_login                = "vscode_login"
    aux_token                   = "token"

    client_app_type = {
        aad_username_password:       ClientAppType.public,
        aad_application_key:         ClientAppType.confidential,
        aad_application_certificate: ClientAppType.confidential,
        aad_code_login:              ClientAppType.public,
    }


_AAD_URL_BY_CLOUD = {
    Cloud.PUBLIC:      "https://login.microsoftonline.com",
    Cloud.MOONCAKE:    "https://login.partner.microsoftonline.cn",  # === 'login.chinacloudapi.cn'?
    Cloud.FAIRFAX:     "https://login.microsoftonline.us",
    Cloud.BLACKFOREST: "https://login.microsoftonline.de",
    Cloud.PPE:         "https://login.windows-ppe.net",
}
_AAD_URL_BY_CLOUD[Cloud.CHINA]      = _AAD_URL_BY_CLOUD[Cloud.MOONCAKE]
_AAD_URL_BY_CLOUD[Cloud.GOVERNMENT] = _AAD_URL_BY_CLOUD[Cloud.FAIRFAX]
_AAD_URL_BY_CLOUD[Cloud.GERMANY]    = _AAD_URL_BY_CLOUD[Cloud.BLACKFOREST]


_CLOUD_BY_HOST_SUFFIX = {
    ".com": Cloud.PUBLIC,
    ".windows.net": Cloud.PUBLIC,
    ".usgovcloudapi.net": Cloud.FAIRFAX,
    ".io":  Cloud.PUBLIC,
    ".us":  Cloud.FAIRFAX,
    ".cn":  Cloud.MOONCAKE,
    ".de":  Cloud.BLACKFOREST
}


_CLOUD_DSTS_AAD_DOMAINS = {
    # Define dSTS domains whitelist based on its Supported Environments & National Clouds list here
    # https://microsoft.sharepoint.com/teams/AzureSecurityCompliance/Security/SitePages/dSTS%20Fundamentals.aspx
    Cloud.PUBLIC:       'dsts.core.windows.net',
    Cloud.MOONCAKE:     'dsts.core.chinacloudapi.cn',  
    Cloud.BLACKFOREST:  'dsts.core.cloudapi.de', 
    Cloud.FAIRFAX:      'dsts.core.usgovcloudapi.net'
}
_CLOUD_DSTS_AAD_DOMAINS[Cloud.CHINA] = _CLOUD_DSTS_AAD_DOMAINS[Cloud.MOONCAKE]
_CLOUD_DSTS_AAD_DOMAINS[Cloud.GOVERNMENT] = _CLOUD_DSTS_AAD_DOMAINS[Cloud.FAIRFAX]
_CLOUD_DSTS_AAD_DOMAINS[Cloud.GERMANY] = _CLOUD_DSTS_AAD_DOMAINS[Cloud.BLACKFOREST]


# shared not cached context per authority
global_msal_client_app = {}

# shared cached context per authority
global_msal_client_app_sso = {}


class _MyAadHelper(AadHelper):

    def __init__(self, kcsb, default_clientid, msal_client_app=None, msal_client_app_sso=None, http_client=None, **options):
        global global_msal_client_app
        global global_msal_client_app_sso

        super(_MyAadHelper, self).__init__(kcsb, default_clientid, msal_client_app, msal_client_app_sso, **options)
        self._http_client = http_client
        self._username = None
        if all([kcsb.aad_user_id, kcsb.password]):
            self._authentication_method = AuthenticationMethod.aad_username_password
            self._username = kcsb.aad_user_id
            self._password = kcsb.password
        elif all([kcsb.application_client_id, kcsb.application_key]):
            self._authentication_method = AuthenticationMethod.aad_application_key
            self._client_secret = kcsb.application_key
            self._client_credential = self._client_secret
        elif all([kcsb.application_client_id, kcsb.application_certificate, kcsb.application_certificate_thumbprint]):
            self._authentication_method = AuthenticationMethod.aad_application_certificate
            self._certificate = kcsb.application_certificate
            self._thumbprint = kcsb.application_certificate_thumbprint
            self._client_credential = {
                "private_key": self._certificate,
                "thumbprint": self._thumbprint,
            }
        else:
            self._authentication_method = AuthenticationMethod.aad_code_login
            self._username = kcsb.aad_user_id  # optional

        self._client_app_type = AuthenticationMethod.client_app_type.get(self._authentication_method)

        # to provide stickiness, to avoid switching tokens when not required
        self._current_token = None
        self._current_msal_client_app = None
        self._current_authentication_method = None
        self._current_client_app_type = None
        self._current_scopes = None
        self._current_username = None

        self._token_claims_cache = (None, None)

        self._try_token_msal_client_app = None
        self._try_azcli_msal_client_app = None
        self._try_azcli_sub_msal_client_app = None
        self._try_msi_msal_client_app = None

        # options are freezed for authentication when object is created, 
        # to eliminate the need to specify auth option on each query, and to modify behavior on exah query
        self._options = {**options}

        # track warning to avoid repeating
        self._displayed_warnings = []

        url = urlparse(kcsb.data_source)
        self._resource = f"{url.scheme}://{url.hostname}"

        self._scopes = [f"{self._resource}/.default"]

        self._authority = kcsb.authority_id or "common"

        self._aad_login_url = self._get_aad_login_url(kcsb.aad_url)

        self._authority_uri = f"{self._aad_login_url}/{self._authority}"

        self._client_id = kcsb.application_client_id or default_clientid

        self._client_app_key = self._create_client_app_key()

        self._set_msal_client_app(msal_client_app=msal_client_app, msal_client_app_sso=msal_client_app_sso)


    def get_details(self):
        details = {
            "scopes": self._current_scopes or self._scopes,
            "authentication_method": self._current_authentication_method,
            "client_app_type": self._current_client_app_type,
            "authority_uri": self._authority_uri,
            "client_id": self._client_id,
            "aad_login_url": self._aad_login_url}
        if self._current_client_app_type == ClientAppType.public:
            details["username"] = self._current_username

        if self._current_token:
            details["scopes"] = self._get_token_scope(self._current_token) or details["scopes"]
            details["authority_uri"] = self._get_token_authority(self._current_token) or self._get_authority_from_token(self._current_token) or details["authority_uri"]
            details["client_id"] = self._get_token_client_id(self._current_token) or self._get_client_id_from_token(self._current_token) or details["client_id"]
            details["username"] = self._get_token_user_id(self._current_token) or self._get_username_from_token(self._current_token) or details["username"]

        if self._current_authentication_method == AuthenticationMethod.azcli_login_subscription:
            details["subscription"] = self._options.get("try_azcli_login_subscription")
        elif self._current_authentication_method == AuthenticationMethod.azcli_login:
            details["tenant"] = self._authority
        elif self._current_authentication_method == AuthenticationMethod.azcli_login_by_profile:
            details["tenant"] = self._authority
        elif self._current_authentication_method == AuthenticationMethod.vscode_login:
            details["tenant"] = self._authority
        elif self._current_authentication_method == AuthenticationMethod.managed_service_identity:
            details["msi_params"] = self._options.get("try_msi")
        elif self._current_authentication_method == AuthenticationMethod.aad_application_certificate:
            details["thumbprint"] = self._thumbprint
            details["certificate"] = '*****' if self._password else 'NOT-SET'
        elif self._current_authentication_method == AuthenticationMethod.aad_application_key:
            details["client_secret"] = '*****' if self._client_secret else 'NOT-SET'
        elif self._current_authentication_method == AuthenticationMethod.aad_username_password:
            details["password"] = '*****' if self._password else 'NOT-SET'

        return details


    def acquire_token(self):
        """Acquire tokens from AAD."""
        acquire_token_result = None
        previous_token = self._current_token
        try:
            if self._current_token is not None:
                self._current_token = self._validate_and_refresh_token(self._current_token)

            if self._current_token is None:
                self._current_authentication_method = None
                self._current_msal_client_app = None
                self._current_client_app_type = None
                self._current_scopes = None
                self._current_username = None

            if self._current_token is None:
                if self._options.get("try_token") is not None:
                    token = self._get_aux_token(token=self._options.get("try_token"))
                    self._current_token = self._validate_and_refresh_token(token)

            if self._current_token is None:
                if self._options.get("try_msi") is not None:
                    token = self._get_msi_token(msi_params=self._options.get("try_msi"))
                    self._current_token = self._validate_and_refresh_token(token)

            if self._current_token is None:
                if self._options.get("try_azcli_login_subscription") is not None:
                    token = self._get_azcli_token(subscription=self._options.get("try_azcli_login_subscription"))
                    self._current_token = self._validate_and_refresh_token(token)

            if self._current_token is None:
                if self._options.get("try_azcli_login"):
                    token = self._get_azcli_token()
                    self._current_token = self._validate_and_refresh_token(token)

            if self._current_token is None:
                if self._options.get("try_azcli_login_by_profile"):
                    token = self._get_azcli_token_by_profile()
                    self._current_token = self._validate_and_refresh_token(token)

            if self._current_token is None:
                if self._options.get("try_vscode_login"):
                    token = self._get_vscode_token()
                    self._current_token = self._validate_and_refresh_token(token)

            if self._current_token is None:
                self._current_authentication_method = self._authentication_method
                self._current_msal_client_app = self._msal_client_app_sso or self._msal_client_app
                self._current_client_app_type = self._client_app_type
                self._current_scopes = self._scopes
                self._current_username = self._username

            if self._current_token is None:
                if self._msal_client_app_sso is not None:
                    self._current_msal_client_app = self._msal_client_app_sso
                    self._current_token = self._acquire_msal_token_silent(sso_flow_code=True)

            if self._current_token is None:
                if self._msal_client_app is not None:
                    self._current_msal_client_app = self._msal_client_app
                    self._current_token = self._acquire_msal_token_silent()

            if self._current_token is None:
                logger().debug("No suitable token exists in cache. Let's get a new one from AAD.")
                self._current_msal_client_app = self._msal_client_app_sso or self._msal_client_app

                if self._authentication_method is AuthenticationMethod.aad_username_password:
                    # See this page for constraints of Username Password Flow.
                    # https://github.com/AzureAD/microsoft-authentication-library-for-python/wiki/Username-Password-Authentication
                    acquire_token_result = self._current_msal_client_app.acquire_token_by_username_password(
                        self._username, self._password, scopes=self._scopes)                        

                elif self._authentication_method is AuthenticationMethod.aad_application_key:
                    acquire_token_result = self._current_msal_client_app.acquire_token_for_client(scopes=self._scopes)

                elif self._authentication_method is AuthenticationMethod.aad_application_certificate:
                    acquire_token_result = self._current_msal_client_app.acquire_token_for_client(scopes=self._scopes)

                elif self._authentication_method is AuthenticationMethod.aad_code_login and (
                    self._options.get("code_auth_interactive_mode") == "auth_code"  or (self._options.get("kernel_location") == "local" and self._options.get("code_auth_interactive_mode") == "auto")):
                    acquire_token_result = self._current_msal_client_app.acquire_token_interactive(
                        scopes=self._scopes,
                        login_hint = self._current_username,
                        prompt="select_account")
                    if TokenResponseFieldsV2.ACCESS_TOKEN in acquire_token_result:
                        logger().debug(f"_MyAadHelper::acquire_token - got token - scopes: '{self._scopes}', client: '{self._client_id}'")
                        self._username = self._username or self._get_token_user_id(acquire_token_result) or self._get_username_from_token(acquire_token_result)
                        self._current_username = self._username

                elif self._authentication_method is AuthenticationMethod.aad_code_login:
                    flow = self._current_msal_client_app.initiate_device_flow(scopes=self._scopes)
                    url = flow[OAuth2DeviceCodeResponseParameters.VERIFICATION_URL]
                    device_code = flow[OAuth2DeviceCodeResponseParameters.USER_CODE].strip()

                    device_code_login_notification = self._options.get("device_code_login_notification")
                    if device_code_login_notification == "auto":
                        if self._options.get("notebook_app") in ["ipython"]:
                            device_code_login_notification = "popup_interaction"
                        elif self._options.get("notebook_app") in ["visualstudiocode", "azuredatastudio", "azuredatastudiosaw"]:
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
                                    device_code_login_notification = "browser_reference"
                            else:
                                device_code_login_notification = "terminal"
                        elif self._options.get("notebook_app") in ["azureml", "azuremljupyternotebook", "azuremljupyterlab"]:
                            device_code_login_notification = "terminal_reference"
                        else:
                            device_code_login_notification = "button"

                    if (self._options.get("kernel_location") == "local"
                            or device_code_login_notification in ["browser", "browser_reference"]
                            or (device_code_login_notification == "popup_interaction" and self._options.get("popup_interaction") == "webbrowser_open_at_kernel")):
                        # copy code to local clipboard
                        try:
                            pyperclip = Dependencies.get_module("pyperclip", dont_throw=True)
                            if pyperclip is not None:
                                pyperclip.copy(device_code)
                        except:
                            pass

                    # if  self._options.get("notebook_app")=="papermill" and self._options.get("login_code_destination") =="browser":
                    #     raise Exception("error: using papermill without an email specified is not supported")
                    if device_code_login_notification == "email":
                        params = Parser.parse_and_get_kv_string(self._options.get('device_code_notification_email'), {})
                        email_notification = EmailNotification(**params)
                        subject = f"Kqlmagic device_code {device_code} authentication (context: {email_notification.context})"
                        resource = self._resource.replace("://", ":// ")  # just to make sure it won't be replace in email by safelinks
                        email_message = f"Device_code: {device_code}\n\nYou are asked to authorize access to resource: {resource}\n\n" \
                                        f"Open the page {url} and enter the code {device_code} to authenticate\n\nKqlmagic"
                        email_notification.send_email(subject, email_message)
                        info_message =f"An email was sent to {email_notification.send_to} with device_code {device_code} to authenticate"
                        Display.showInfoMessage(info_message, display_handler_name='acquire_token', **self._options)

                    elif device_code_login_notification in ["browser", "browser_reference", "terminal", "terminal_reference"]:
                        if device_code_login_notification in ["browser_reference", "terminal_reference"]:
                            html_str = (
                                f"""<!DOCTYPE html>
                                <html><body>
                                <input  id="kql_MagicCodeAuthInput" type="text" readonly style="font-weight: bold; border: none;" size={single_quote(len(device_code))} value={single_quote(device_code)}>

                                <script>
                                function kql_MagicCopyCodeFunction() {{
                                    /* Get the text field */
                                    var copyText = document.getElementById("kql_MagicCodeAuthInput");

                                    /* Select the text field */
                                    copyText.select();

                                    /* Copy the text inside the text field */
                                    document.execCommand("copy");

                                    /* Alert the copied text */
                                    // alert("Copied the text: " + copyText.value);
                                }}
                                kql_MagicCopyCodeFunction()
                                </script>

                                </body></html>"""
                            )
                            Display.show_html(html_str, display_handler_name='acquire_token', **self._options)
                        else:
                            # this print is not for debug
                            print(device_code)
                            sys.stdout.flush()

                        if device_code_login_notification in ["browser", "browser_reference"]:
                            input(f"Copy code to clipboard and press any key to open browser")
                            OsDependentAPI.webbrowser_open(flow[OAuth2DeviceCodeResponseParameters.VERIFICATION_URL])
                        else:
                            msg = f"Copy code to clipboard and authenticate here: {flow[OAuth2DeviceCodeResponseParameters.VERIFICATION_URL]}"
                            print(msg)
                            sys.stdout.flush()
                            input("Press Enter after signing in from another device to proceed, CTRL+C to abort.")

                    elif device_code_login_notification == "popup_interaction" and self._options.get("popup_interaction") != "memory_button":
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

                    else:  # device_code_login_notification == "button":
                        html_str = (
                            f"""<!DOCTYPE html>
                            <html><body>

                            <!-- h1 id="user_code_p"><b>{device_code}</b><br></h1-->

                            <input  id="kql_MagicCodeAuthInput" type="text" readonly style="font-weight: bold; border: none;" size={single_quote(len(device_code))} value={single_quote(device_code)}>

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

                    #
                    # wait for flow to finish
                    #
                    try:
                        # Ideally you should wait here, in order to save some unnecessary polling
                        # input("Press Enter after signing in from another device to proceed, CTRL+C to abort.")
                        acquire_token_result = self._current_msal_client_app.acquire_token_by_device_flow(flow)  # By default it will block
                        # You can follow this instruction to shorten the block time
                        #    https://msal-python.readthedocs.io/en/latest/#msal.PublicClientApplication.acquire_token_by_device_flow
                        # or you may even turn off the blocking behavior,
                        # and then keep calling acquire_token_by_device_flow(flow) in your own customized loop.
                        if TokenResponseFieldsV2.ACCESS_TOKEN in acquire_token_result:
                            logger().debug(f"_MyAadHelper::acquire_token - got token - scopes: '{self._scopes}', client: '{self._client_id}'")
                            self._username = self._username or self._get_token_user_id(acquire_token_result) or self._get_username_from_token(acquire_token_result)
                            self._current_username = self._username

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

                if acquire_token_result and TokenResponseFieldsV2.ACCESS_TOKEN in acquire_token_result:
                    self._current_token = acquire_token_result

            if self._current_token is None:
                raise AuthenticationError("Failed to create token", acquire_token_result=acquire_token_result)

            if self._current_token != previous_token:
                self._warn_token_diff_from_conn_str()
            else:
                logger().debug(f"_MyAadHelper::acquire_token - valid token exist - scopes: '{self._scopes}', username: '{self._username}', client: '{self._client_id}'")

            return self._create_authorization_header()
        except Exception as e:
            kwargs = self._get_authentication_error_kwargs()
            raise AuthenticationError(e, **kwargs)


    def _acquire_msal_token_silent(self, client_app_type=None, msal_client_app=None, scopes=None, username=None, sso_flow_code=None)->dict:
        token = None
        account = None
        client_app_type = client_app_type or self._current_client_app_type
        msal_client_app = msal_client_app or self._current_msal_client_app
        scopes = scopes or self._current_scopes
        if client_app_type == ClientAppType.public:
            username = username or self._current_username
            if username or sso_flow_code:
                accounts = msal_client_app.get_accounts(username=username)
                if accounts:
                    logger().debug("Account(s) exists in cache, probably with token too. Let's try.")
                    account = accounts[0]
        if client_app_type == ClientAppType.confidential or account:
            logger().debug("Account(s) exists in cache, probably with token too. Let's try.")
            token = msal_client_app.acquire_token_silent(scopes, account) 
        return token 


    #
    # Assume OAuth2 format (e.g. MSI Token) too
    #
    def _get_token_access_token(self, token:dict, default_access_token:str=None)->str:
        return (token.get(TokenResponseFieldsV1.ACCESS_TOKEN)
                or token.get(TokenResponseFieldsV2.ACCESS_TOKEN)
                or token.get(OAuth2TokenFields.ACCESS_TOKEN)
                or default_access_token)


    def _get_token_client_id(self, token:dict, default_client_id:str=None)->str:
        return (token.get(TokenResponseFieldsV1._CLIENT_ID)
                or token.get(OAuth2TokenFields.CLIENT_ID)
                or default_client_id)


    def _get_token_expires_on(self, token:dict, default_expires_on:str=None)->str:
        expires_on = default_expires_on
        if token.get(TokenResponseFieldsV1.EXPIRES_ON) is not None:
            expires_on = token.get(TokenResponseFieldsV1.EXPIRES_ON)
        elif token.get(OAuth2TokenFields.EXPIRES_ON) is not None:
            try:
                # The date is represented as the number of seconds from "1970-01-01T0:0:0Z UTC" (corresponds to the token's exp claim).
                expires_on = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(token.get(OAuth2TokenFields.EXPIRES_ON)))
            except:
                # this happens if the expires_on field is empty or is not a float (i.e datetime)
                expires_on = None
        return expires_on


    def _get_token_not_before(self, token:dict, default_not_before:str=None)->str:
        not_before = default_not_before
        if token.get(OAuth2TokenFields.NOT_BEFORE) is not None:
            # The date is represented as the number of seconds from "1970-01-01T0:0:0Z UTC" (corresponds to the token's nbf claim).
            not_before = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(token.get(OAuth2TokenFields.NOT_BEFORE)))
        return not_before


    def _get_token_token_type(self, token:dict, default_token_type:str=None)->str:
        return (token.get(TokenResponseFieldsV1.TOKEN_TYPE)
                or token.get(TokenResponseFieldsV2.TOKEN_TYPE)
                or token.get(OAuth2TokenFields.TOKEN_TYPE)
                or default_token_type)


    def _get_token_resource(self, token:dict, default_resource:str=None)->str:
        resource = token.get(TokenResponseFieldsV1.RESOURCE)
        if resource is None:
            scope = token.get(TokenResponseFieldsV2.SCOPE)
            if scope is not None:
                resource = '/'.join(scope.split(None, 1)[0].spilt('/')[:3])
        return token.get(TokenResponseFieldsV1.RESOURCE) or token.get(OAuth2TokenFields.RESOURCE) or default_resource


    def _get_token_scope(self, token:dict, default_scopes:str=None)->list:
        scope = token.get(TokenResponseFieldsV2.SCOPE)
        if scope is None:
            resource = self._get_token_resource(token) or self._get_resources_from_token(token) or self._resource
            if resource is not None:
                scope = f"{resource}/.default"
        scope = scope
        return scope.split() if scope is not None else default_scopes


    def _get_token_user_id(self, token:dict, default_user_id:str=None)->str:
        return token.get(TokenResponseFieldsV1.USER_ID) or default_user_id


    def _get_token_refresh_token(self, token:dict, default_refresh_token:str=None)->str:
        return (token.get(TokenResponseFieldsV1.REFRESH_TOKEN)
                or token.get(TokenResponseFieldsV2.REFRESH_TOKEN)
                or token.get(OAuth2TokenFields.REFRESH_TOKEN)
                or default_refresh_token)


    def _get_token_id_token(self, token:dict, default_id_token:str=None)->str:
        return (token.get(OAuth2TokenFields.ID_TOKEN)
                or token.get(TokenResponseFieldsV2.ID_TOKEN)
                or default_id_token)


    def _get_token_authority(self, token:dict, default_authority:str=None)->str:
        return token.get(TokenResponseFieldsV1._AUTHORITY) or default_authority


    def _create_authorization_header(self)->str:
        "create content for http authorization header"
        access_token = self._get_token_access_token(self._current_token)
        if access_token is None:
            raise AuthenticationError("Not a valid token, property 'access_token' is not present.")

        token_type = self._get_token_token_type(self._current_token)
        if token_type is None:
            raise AuthenticationError("Unable to determine the token type. Neither 'tokenType' nor 'token_type' property is present.")

        return f"{token_type} {access_token}"


    def _get_username_from_token(self, token:dict)->str:
        "retrieves username from in id token or access token claims"
        id_token_claims = self._get_token_claims(self._get_token_id_token(token))
        access_token_claims = self._get_token_claims(self._get_token_access_token(token))
        claims = {**access_token_claims, **id_token_claims}
        username = claims.get("preferred_username") or claims.get("unique_name") or claims.get("upn") or claims.get("email") or claims.get("sub")
        return username


    def _get_expires_on_from_token(self, token:dict)->str:
        "retrieve expires_on from access token claims"
        expires_on = None
        claims = self._get_token_claims(self._get_token_access_token(token))
        exp = claims.get("exp")
        if exp is not None:
            expires_on = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(exp))
        return expires_on


    def _get_not_before_from_token(self, token:dict)->str:
        "retrieve not_before from access token claims"
        not_before = None
        claims = self._get_token_claims(self._get_token_access_token(token))
        nbf = claims.get("nbf")
        if nbf is not None:
            not_before = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(nbf))
        return not_before


    def _get_client_id_from_token(self, token:dict)->str:
        "retrieve client_id from access token claims"
        claims = self._get_token_claims(self._get_token_access_token(token))
        client_id = claims.get("client_id") or claims.get("appid") or claims.get("azp")
        return client_id

    
    def _get_resources_from_token(self, token:dict)->list:
        "retrieve resource list from access token claims"
        resources = None
        claims = self._get_token_claims(self._get_token_access_token(token))
        resources = claims.get("aud")
        if type(resources) == str:
            resources = [resources]
        return resources


    def _get_authority_from_token(self, token:OAuth2DeviceCodeResponseParameters)->str:
        "retrieve authority_uri from access token claims"
        authority_uri = None
        try:
            claims = self._get_token_claims(self._get_token_access_token(token))
            tenant_id = claims.get("tid")
            issuer = claims.get("iss")

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
        except:
            pass

        return authority_uri


    def _get_aux_token(self, token:dict)->str:
        "retrieve token from aux token"
        self._current_authentication_method = AuthenticationMethod.aux_token
        try:
            token = token
            self._migrate_to_msal_app(token)
        except:
            pass
        logger().debug(f"_MyAadHelper::_get_aux_token {'failed' if token is None else 'succeeded'} to get token")
        return token


    def _get_vscode_token(self)->str:
        token = None
        tenant = None if self._authority == "common" else self._authority
        self._current_authentication_method = AuthenticationMethod.vscode_login

        old_stderr = sys.stderr
        sys.stderr = StringIO()
        try:
            from azure.identity import VisualStudioCodeCredential  # pylint: disable=no-name-in-module, import-error
            # in AppData/Roaming/Code/User/settings.json the key "azure.cloud" should be set to "AzureCloud"
            # should match the key: "VS Code Azure/AzureCloud" in windows CredentialManager (Control Pannel)
            
            vscode = VisualStudioCodeCredential(tenant_id=tenant, authority=self._get_aad_login_url())
            t = vscode.get_token(f"{self._resource}/.default")
            if t:
                token = {"access_token": t.token, "token_type": "bearer"}
                sys.stderr = old_stderr or sys.stderr
                return token
        except:
            pass
        sys.stderr = old_stderr or sys.stderr

        logger().debug(f"_MyAadHelper::_get_vscode_token {'failed' if token is None else 'succeeded'} to get token - tenant: '{tenant}'")
        return token


    def _get_azcli_token(self, subscription:str=None)->str:
        "retrieve token from azcli login"
        token = None
        tenant = self._authority if subscription is None else None
        self._current_authentication_method = AuthenticationMethod.azcli_login_subscription if subscription is not None else AuthenticationMethod.azcli_login

        old_stderr = sys.stderr
        sys.stderr = StringIO()

        try:
            # azure.identity support only current account / tenant / subscription
            if subscription is None and (tenant is None or tenant == "common"):
                from azure.identity import AzureCliCredential  # pylint: disable=no-name-in-module, import-error
                azure_cli = AzureCliCredential()
                t = azure_cli.get_token(f"{self._resource}/.default")
                if t is not None and t.token is not None:
                    token = {"access_token": t.token, "token_type": "bearer"}
        except:
            pass

        try:
            if token is None:
                token = self._get_azcli_token_by_profile(subscription)
                self._current_authentication_method = AuthenticationMethod.azcli_login_subscription if subscription is not None else AuthenticationMethod.azcli_login
        except:
            pass
        
        sys.stderr = old_stderr or sys.stderr

        logger().debug(f"_MyAadHelper::_get_azcli_token {'failed' if token is None else 'succeeded'} to get token - subscription: '{subscription}', tenant: '{tenant}'")
        return token


    def _get_azcli_token_by_profile(self, subscription:str=None)->str:
        "retrieve token from azcli login"
        token = None
        tenant = self._authority if subscription is None else None
        self._current_authentication_method = AuthenticationMethod.azcli_login_by_profile
        
        old_stderr = sys.stderr
        sys.stderr = StringIO()

        try:
            from azure.common.credentials import get_cli_profile  # pylint: disable=no-name-in-module, import-error
            profile = get_cli_profile()
            credential, _subscription, _tenant = profile.get_raw_token(resource=self._resource, subscription=subscription, tenant=tenant)
            token_type, access_token, token = credential  # pylint: disable=unused-variable
            self._migrate_to_msal_app(token)
            self._current_msal_client_app = None
        except:
            pass
        sys.stderr = old_stderr or sys.stderr

        logger().debug(f"_MyAadHelper::_get_azcli_token_by_profile {'failed' if token is None else 'succeeded'} to get token - subscription: '{subscription}', tenant: '{tenant}'")
        return token


    def _get_msi_token(self, msi_params=None)->str:
        "retrieve token from managed service identity"
        msi_params = msi_params or {}
        token = None
        self._current_authentication_method = AuthenticationMethod.managed_service_identity

        old_stderr = sys.stderr
        sys.stderr = StringIO()
        try:
            from msrestazure.azure_active_directory import MSIAuthentication
            # allow msi_params to overrite the connection string resource
            credentials = MSIAuthentication(**{"resource":self._resource, **msi_params})
            token = credentials.token
            self._migrate_to_msal_app(token)
        except:
            pass
        sys.stderr = old_stderr or sys.stderr

        logger().debug(f"_MyAadHelper::_get_msi_token {'failed' if token is None else 'succeeded'} to get token - msi_params: '{msi_params}'")
        return token


    def _migrate_to_msal_app(self, token:dict):
        if token is not None:
            refresh_token = self._get_token_refresh_token(token)
            if refresh_token is not None:
                msal = Dependencies.get_module("msal", dont_throw=True)
                scopes = self._get_token_scope(token) or self._scopes
                authority_uri = self._get_token_authority(token) or self._get_authority_from_token(token) or self._authority_uri
                client_id = self._get_token_client_id(token) or self._get_client_id_from_token(token) or self._client_id
                username = self._get_token_user_id(token) or self._get_username_from_token(token)

                _tk = {x:"XXXXX" if x.lower().endswith("token") else token[x] for x in token}
                claims = self._get_token_claims(self._get_token_access_token(token))
                logger().debug(f"_MyAadHelper::_migrate_to_msal_app token: {json.dumps(_tk)}")
                logger().debug(f"_MyAadHelper::_migrate_to_msal_app accessToken claims: {json.dumps(claims)}")
                logger().debug(f"_MyAadHelper::_migrate_to_msal_app _get_token_scope: {self._get_token_scope(token)}")
                logger().debug(f"_MyAadHelper::_migrate_to_msal_app _get_token_authority: {self._get_token_authority(token)}")
                logger().debug(f"_MyAadHelper::_migrate_to_msal_app _get_authority_from_token: {self._get_authority_from_token(token)}")
                logger().debug(f"_MyAadHelper::_migrate_to_msal_app _get_token_client_id: {self._get_token_client_id(token)}")
                logger().debug(f"_MyAadHelper::_migrate_to_msal_app _get_client_id_from_token: {self._get_client_id_from_token(token)}")
                logger().debug(f"_MyAadHelper::_migrate_to_msal_app _get_token_user_id: {self._get_token_user_id(token)}")
                logger().debug(f"_MyAadHelper::_migrate_to_msal_app _get_username_from_token: {self._get_username_from_token(token)}")

                logger().debug(f"_MyAadHelper::_migrate_to_msal_app scopes: {scopes}")
                logger().debug(f"_MyAadHelper::_migrate_to_msal_app authority_uri: {authority_uri}")
                logger().debug(f"_MyAadHelper::_migrate_to_msal_app client_id: {client_id}")
                logger().debug(f"_MyAadHelper::_migrate_to_msal_app username: {username}")


                # print(f">>> token {json.dumps(_tk)}")
                # print(f">>> accessToken claims {json.dumps(claims)}")
                # print(f">>> _get_token_scope: {self._get_token_scope(token)}")
                # print(f">>> _get_token_authority: {self._get_token_authority(token)}")
                # print(f">>> _get_authority_from_token: {self._get_authority_from_token(token)}")
                # print(f">>> _get_token_client_id: {self._get_token_client_id(token)}")
                # print(f">>> _get_client_id_from_token: {self._get_client_id_from_token(token)}")
                # print(f">>> _get_token_user_id: {self._get_token_user_id(token)}")
                # print(f">>> _get_username_from_token: {self._get_username_from_token(token)}")

                app = msal.PublicClientApplication(client_id, authority=authority_uri, http_client=self._http_client)
                result = app.acquire_token_by_refresh_token(refresh_token, scopes)  # pylint: disable=no-member
                if "error" in result:
                    self._warn_on_token_validation_failure(f"failed to migrate token to msal app: {result}")
                else:
                    valid_token = self._acquire_msal_token_silent(client_app_type=ClientAppType.public, msal_client_app=app, scopes=scopes, username=username)
                    _vtk = {x:"XXXXX" if x.lower().endswith("token") else valid_token[x] for x in valid_token}
                    vclaims = self._get_token_claims(self._get_token_access_token(valid_token))
                    logger().debug(f"_MyAadHelper::_migrate_to_msal_app migratedToken: {json.dumps(_vtk)}")
                    logger().debug(f"_MyAadHelper::_migrate_to_msal_app migratedAccessToken claims: {json.dumps(vclaims)}")
                    # print(f">>> migratedToken {json.dumps(_vtk)}")
                    # print(f">>> migratedAccessToken claims {json.dumps(vclaims)}")
                    if valid_token is not None:
                        self._current_msal_client_app = app
                        self._current_client_app_type = ClientAppType.public
                        self._current_scopes = scopes
                        self._current_username = username


    def _validate_and_refresh_token(self, token:dict)->dict:
        "validate token is valid to use now. Now is between not_before and expires_on. If exipred try to refresh"
        valid_token = None    
        if token is not None:
            if self._current_msal_client_app is not None:
                valid_token = self._acquire_msal_token_silent()
                # print(">>> _acquire_msal_token_silent")
            else:
                scopes = self._get_token_scope(token) or self._scopes
                not_before = self._get_token_not_before(token) or self._get_not_before_from_token(token)
                if not_before is not None:
                    not_before_datetime = dateutil.parser.parse(not_before)
                    current_datetime = datetime.now() - timedelta(minutes=1)
                    if not_before_datetime > current_datetime:
                        logger().debug(f"_MyAadHelper::_validate_and_refresh_token - failed - token can be used not before {not_before} - scopes: '{scopes}'")
                        self._warn_on_token_validation_failure(f"access token cannot be used before {not_before}")
                        return None

                expires_on = self._get_token_expires_on(token) or self._get_expires_on_from_token(token)
                if expires_on is not None:
                    expiration_datetime = dateutil.parser.parse(expires_on)
                else:
                    expiration_datetime = datetime.now() + timedelta(minutes=30)

                current_datetime = datetime.now() + timedelta(minutes=1)
                if expiration_datetime > current_datetime:
                    valid_token = token
                    logger().debug(f"_MyAadHelper::_validate_and_refresh_token - succeeded, no need to refresh yet, expires on {expires_on} - scopes: '{scopes}'")
                else:
                    logger().debug(f"_MyAadHelper::_validate_and_refresh_token - token expires on {expires_on} need to refresh - scopes: '{scopes}'")

        return valid_token


    def _create_client_app_key(self):
        client_app_key = {
            "client_app_type": self._client_app_type,
            "client_id": self._client_id, 
            "authority_uri": self._authority_uri
        }
        if self._client_app_type == ClientAppType.confidential:
            if self._authentication_method == AuthenticationMethod.aad_application_key:
                client_app_key["confidential"] = self._client_secret
            elif self._authentication_method == AuthenticationMethod.aad_application_certificate:
                client_app_key["confidential"] = self._thumbprint

        return f"{self._authority_uri}/{self._client_id}/{self._client_app_type}"
        # return json.dumps(client_app_key)

    def _create_client_app(self, cache=None):
        client_app = None
        msal = Dependencies.get_module("msal", dont_throw=True)
        if msal:
            if self._client_app_type == ClientAppType.public:
                client_app = msal.PublicClientApplication(self._client_id, authority=self._authority_uri, token_cache=cache, http_client=self._http_client)
            elif self._client_app_type == ClientAppType.confidential:
                client_app = msal.ConfidentialClientApplication(self._client_id, authority=self._authority_uri, client_credential=self._client_credential, token_cache=cache, http_client=self._http_client)
        return client_app



    def _set_msal_client_app(self, msal_client_app=None, msal_client_app_sso=None):
        "set the msal application"
        global global_msal_client_app
        global global_msal_client_app_sso

        self._msal_client_app = msal_client_app
        if self._msal_client_app is None:
            if global_msal_client_app.get(self._client_app_key) is None:
                global_msal_client_app[self._client_app_key] = self._create_client_app(cache=None)
            self._msal_client_app = global_msal_client_app.get(self._client_app_key)

        self._msal_client_app_sso = None
        if self._options.get("enable_sso"):
            self._msal_client_app_sso = msal_client_app_sso
            if self._msal_client_app_sso is None:
                if global_msal_client_app_sso.get(self._client_app_key) is None:
                    cache = MsalTokenCache.get_cache(self._client_app_key, **self._options)
                    if cache is not None:
                        global_msal_client_app_sso[self._client_app_key] = self._create_client_app(cache=cache)
                self._msal_client_app_sso = global_msal_client_app_sso.get(self._client_app_key)


    def _get_cloud_from_resource(self):
        for host_suffix in _CLOUD_BY_HOST_SUFFIX:
            if self._resource.endswith(host_suffix):
                return _CLOUD_BY_HOST_SUFFIX[host_suffix]
        return self._options.get("cloud")


    def _get_aad_login_url(self, aad_login_url=None):
        if aad_login_url is None:
            cloud = self._get_cloud_from_resource()
            aad_login_url = _AAD_URL_BY_CLOUD.get(cloud)
            if aad_login_url is None:
                raise KqlEngineError(f"AAD is not known for this cloud '{cloud}', please use aadurl property in connection string.")
        return aad_login_url


    def _warn_on_token_validation_failure(self, message)->None:
        if self._options.get("auth_token_warnings"):
            if self._current_authentication_method is not None and message is not None:
                warn_message =f"Can't use '{self._current_authentication_method}' token entry, {message}'"
                Display.showWarningMessage(warn_message, display_handler_name='acquire_token', **self._options)


    def _warn_token_diff_from_conn_str(self)->None:
        if self._options.get("auth_token_warnings"):
            token = self._current_token
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


    def _get_authentication_error_kwargs(self):
        " collect info for AuthenticationError exception and raise it"
        kwargs = {}
        if self._current_authentication_method is AuthenticationMethod.aad_username_password:
            kwargs = {"username": self._username, "client_id": self._client_id}
        elif self._current_authentication_method is AuthenticationMethod.aad_application_key:
            kwargs = {"client_id": self._client_id}
        elif self._current_authentication_method is AuthenticationMethod.aad_code_login:
            kwargs = {"client_id": self._client_id}
        elif self._current_authentication_method is AuthenticationMethod.aad_application_certificate:
            kwargs = {"client_id": self._client_id, "thumbprint": self._thumbprint}
        elif self._current_authentication_method is AuthenticationMethod.managed_service_identity:
            kwargs = self._options.get("try_msi")
        elif self._current_authentication_method is AuthenticationMethod.azcli_login:
            pass
        elif self._current_authentication_method is AuthenticationMethod.azcli_login_by_profile:
            pass
        elif self._current_authentication_method is AuthenticationMethod.vscode_login:
            pass
        elif self._current_authentication_method is AuthenticationMethod.azcli_login_subscription:
            kwargs = {"subscription": self._options.get("try_azcli_login_subscription")}
        elif self._current_authentication_method is AuthenticationMethod.aux_token:
            token_dict = {}
            for key in self._options.get("try_token"):
                if key in [TokenResponseFieldsV1.ACCESS_TOKEN, TokenResponseFieldsV2.ACCESS_TOKEN, OAuth2TokenFields.ACCESS_TOKEN, TokenResponseFieldsV1.REFRESH_TOKEN, TokenResponseFieldsV2.REFRESH_TOKEN, OAuth2TokenFields.REFRESH_TOKEN, TokenResponseFieldsV2.ID_TOKEN, OAuth2TokenFields.ID_TOKEN]:
                    # obfuscated
                    token_dict[key] = f"..."                   
                else:
                    token_dict[key] = self._options.get("try_token")[key]
            kwargs = token_dict
        else:
            pass

        authority = None
        if self._current_msal_client_app is not None:
            authority = self._current_msal_client_app.authority
        elif self._current_authentication_method == self._authentication_method:
            authority =  self._authority_uri
        elif self._current_token is not None:
            authority = self._get_authority_from_token(self._current_token)
        else:
            authority = authority or self._current_authentication_method

        if authority is None:
            if self._current_authentication_method in [AuthenticationMethod.managed_service_identity, AuthenticationMethod.azcli_login_subscription, AuthenticationMethod.aux_token]:
                authority = self._current_authentication_method
            else:
                authority = self._authority_uri

        kwargs["authority"] = authority
        kwargs["authentication_method"] = self._current_authentication_method
        kwargs["resource"] = self._resource

        return kwargs


    def _get_token_claims(self, jwt_token:str)->dict:
        "get the claims from the token. To optimize it caches the last token/claims"
        claims_token, claims = self._token_claims_cache
        if jwt_token == claims_token:
            return claims
        claims = {}
        if jwt_token:
            try:
                base64_header, base64_claims, _ = jwt_token.split('.')  # pylint: disable=unused-variable
                json_claims = self.base64url_decode(base64_claims)
                claims = json.loads(json_claims)
            except Exception as e:
                # this print is not for debug
                print(f"claims error: {e}")
                pass
            self._token_claims_cache = (jwt_token, claims)
        return claims


    def base64url_decode(self, url_str:str)->str:
        size = len(url_str) % 4
        if size != 0:
            padding_size = 4 - size
            if padding_size == 2:
                url_str += '=='
            elif padding_size == 1:
                url_str += '='
            else:
                raise ValueError(f"Invalid base64 url string: {url_str}")
        return urlsafe_b64decode(url_str.encode('utf-8')).decode('utf-8')

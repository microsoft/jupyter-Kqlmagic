""" A module to acquire tokens from AAD.
"""

from datetime import timedelta, datetime
# import webbrowser
import dateutil.parser
from adal import AuthenticationContext
from kql.display  import Display



class _MyAadHelper(object):
    def __init__(self, kusto_cluster, client_id=None, client_secret=None, username=None, password=None, authority=None):
        self.adal_context = AuthenticationContext('https://login.windows.net/{0}'.format(authority or 'microsoft.com'))
        self.kusto_cluster = kusto_cluster
        self.client_id = client_id or "db662dc1-0cfe-4e1c-a843-19a68e65be58"
        self.client_secret = client_secret
        self.username = username
        self.password = password

    def acquire_token(self):
        """ A method to acquire tokens from AAD. """
        # print("my_aad_helper_acquire_token")
        token_response = self.adal_context.acquire_token(self.kusto_cluster, self.username, self.client_id)

        if token_response is not None:
            expiration_date = dateutil.parser.parse(token_response['expiresOn'])
            if expiration_date > datetime.utcnow() + timedelta(minutes=5):
                return token_response['accessToken']

        if self.client_secret is not None and self.client_id is not None:
            token_response = self.adal_context.acquire_token_with_client_credentials(
                self.kusto_cluster,
                self.client_id,
                self.client_secret)
        elif self.username is not None and self.password is not None:
            token_response = self.adal_context.acquire_token_with_username_password(
                self.kusto_cluster,
                self.username,
                self.password,
                self.client_id)
        else:
            code = self.adal_context.acquire_user_code(self.kusto_cluster, self.client_id)


            url = code['verification_url']
            device_code = code["user_code"].strip()

            html_str = """<!DOCTYPE html>
                <html><body>

                <!-- h1 id="user_code_p"><b>""" +device_code+ """</b><br></h1-->

                <input  id="kqlMagicCodeAuthInput" type="text" readonly style="font-weight: bold; border: none;" size = '""" +str(len(device_code))+ """' value='""" +device_code+ """'>

                <button id='kqlMagicCodeAuth_button', onclick="this.style.visibility='hidden';kqlMagicCodeAuthFunction()">Copy code to clipboard and authenticate</button>

                <script>
                var kqlMagicUserCodeAuthWindow = null
                function kqlMagicCodeAuthFunction() {
                    /* Get the text field */
                    var copyText = document.getElementById("kqlMagicCodeAuthInput");

                    /* Select the text field */
                    copyText.select();

                    /* Copy the text inside the text field */
                    document.execCommand("copy");

                    /* Alert the copied text */
                    // alert("Copied the text: " + copyText.value);

                    var w = screen.width / 2;
                    var h = screen.height / 2;
                    params = 'width='+w+',height='+h
                    kqlMagicUserCodeAuthWindow = window.open('""" +url+ """', 'kqlMagicUserCodeAuthWindow', params);
                }
                </script>

                </body></html>"""

            Display.show(html_str)
            # webbrowser.open(code['verification_url'])
            try:
                token_response = self.adal_context.acquire_token_with_device_code(self.kusto_cluster, code, self.client_id)
            finally:
                html_str = """<!DOCTYPE html>
                    <html><body><script>

                        // close authentication window
                        if (kqlMagicUserCodeAuthWindow && kqlMagicUserCodeAuthWindow.opener != null && !kqlMagicUserCodeAuthWindow.closed) {
                            kqlMagicUserCodeAuthWindow.close()
                        }
                        // clear output cell 
                        Jupyter.notebook.clear_output(Jupyter.notebook.get_selected_index())

                    </script></body></html>"""

                Display.show(html_str)

        return token_response['accessToken']


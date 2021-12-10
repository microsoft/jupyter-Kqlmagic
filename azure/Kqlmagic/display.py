# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

import base64
import uuid
import webbrowser
import urllib.parse
import urllib.request


from ._debug_utils import debug_print
from .ipython_api import display, HTML
from .ipython_api import IPythonAPI
from .my_utils import adjust_path, adjust_path_to_uri, json_dumps, single_quote
from .constants import Constants


try:
    from pygments import highlight
    from pygments.lexers.data import JsonLexer
    from pygments.formatters.terminal import TerminalFormatter
except:
    highlight = None
    JsonLexer = None
    TerminalFormatter = None


class FormattedJsonDict(dict):

    def __init__(self, item, *args, **kwargs):
        super(FormattedJsonDict, self).__init__()
        self.row = None
        if type(item).__name__ == 'KqlRow':
            self.row = item
            item = item.row
        self.update(item)

        _dict = self
        if len(item) == 1 and isinstance(self.get(" "), list):
            _dict = self.get(" ")
        formatted_json = json_dumps(_dict, indent=4, sort_keys=True)
        self.colorful_json = formatted_json

        if highlight and JsonLexer and TerminalFormatter:
            try:
                self.colorful_json = highlight(formatted_json.encode("UTF-8"), JsonLexer(), TerminalFormatter())
            except:
                pass

    def get(self, key, default=None):
        value = super(FormattedJsonDict, self).get(key, default)
        return _getitem_FormattedJson(value)


    def __getitem__(self, key):
        if self.row is not None:
            value = self.row.__getitem__(key)
        else:
            value = self.get(key)
        return _getitem_FormattedJson(value)


    def __repr__(self):
        return self.colorful_json


class FormattedJsonList(list):

    def __init__(self, item, key, *args, **kwargs):
        super(FormattedJsonList, self).__init__()
        self.extend(item)
        self.item = item
        self.key = key or " "
        # formatted_json = json_dumps(self.item, indent=4, sort_keys=True)
        # self.colorful_json = highlight(formatted_json.encode("UTF-8"), JsonLexer(), TerminalFormatter())


    def __getitem__(self, key):
        value = super(FormattedJsonList, self).__getitem__(key)
        return _getitem_FormattedJson(value)


    def __repr__(self):
        if len(self.item) > 0 and type(self.item[0]).__name__ == 'KqlRow':
            _list = [kql_row.row for kql_row in self.item]
        else:
            _list = self.item
        return FormattedJsonDict({self.key: _list}).__repr__()
        # return self.colorful_json


def _getitem_FormattedJson(item, key=None):
    if isinstance(item, list):
        return FormattedJsonList(item, key)
    elif isinstance(item, dict) or type(item).__name__ == "KqlRow":
        return FormattedJsonDict(item)
    else:
        return item


class JSONDict(dict):

    def __init__(self, item, *args, **kwargs):
        super(JSONDict, self).__init__()
        self.item = item
        self.row = None
        if type(item).__name__ == "KqlRow":
            self.row = item
            item = item.row
        self.update(item)


    def get(self, key, default=None):
        value = super(JSONDict, self).get(key, default)
        return _getitem_JSON(value, key or default)


    def __getitem__(self, key):
        if self.row is not None:
            value = self.row.__getitem__(key)
        else:
            value = self.get(key)
        return _getitem_JSON(value, key)

    def _repr_json_(self):
        return self


    def __repr__(self):
        return FormattedJsonDict(self.item).__repr__()


class JSONList(list):

    def __init__(self, item, key, *args, **kwargs):
        super(JSONList, self).__init__()
        self.item = item
        self.extend(item)
        self.key = key or " "


    def __getitem__(self, key):
        value = super(JSONList, self).__getitem__(key)
        return _getitem_JSON(value, key)


    def _repr_json_(self):
        if len(self) > 0 and type(self.item[0]).__name__ == "KqlRow":
            _list = [kql_row.row for kql_row in self.item]
        else:
            _list = self.item
        return JSONDict({self.key: _list})._repr_json_()

    def __repr__(self):
        return FormattedJsonList(self.item, self.key).__repr__()


def _getitem_JSON(item, key=None):
    if isinstance(item, list):
        return JSONList(item, key)
    elif isinstance(item, dict) or type(item).__name__ == "KqlRow":
        return JSONDict(item)
    else:
        return item


class Display(object):
    """
    """

    success_style = {"color": "#417e42", "background-color": "#dff0d8", "border-color": "#d7e9c6"}
    danger_style = {"color": "#b94a48", "background-color": "#f2dede", "border-color": "#eed3d7"}
    info_style = {"color": "#3a87ad", "background-color": "#d9edf7", "border-color": "#bce9f1"}
    warning_style = {"color": "#8a6d3b", "background-color": "#fcf8e3", "border-color": "#faebcc"}
    ref_style = {
        "padding": "2px 6px 2px 6px",
        "color": "#333333",
        "background-color": "#EEEEEE",
        "border-top": "1px solid #CCCCCC", 
        "border-right": "1px solid #333333", 
        "border-bottom": "1px solid #333333", 
        "border-left": "1px solid #CCCCCC"
    }

    showfiles_url_base_path = None
    showfiles_file_base_path = None
    showfiles_folder_name = None


    @staticmethod
    def show_html(html_str, display_handler_name=None, **options):
        Display.show_html_obj(HTML(html_str), display_handler_name=display_handler_name, **options)


    @staticmethod
    def show_html_obj(html_obj, display_handler_name=None, **options) -> object:
        if display_handler_name is None:
            display(html_obj)
        else:
            dh = options["display_handlers"].get(display_handler_name)
            if dh is not None:
                dh.update(html_obj)
            else:
                options["display_handlers"][display_handler_name] = display(html_obj, display_id=options.get("display_id"))
        

    @staticmethod
    def show(content, display_handler_name=None, **options):
        if isinstance(content, str) and len(content) > 0:
            if options is not None and options.get("popup_window", False):
                file_name = Display._get_name(**options)
                file_path = Display._html_to_file_path(content, file_name, **options)
                Display.show_window(file_name, file_path, display_handler_name=display_handler_name, **options)
            else:
                Display.show_html(content, display_handler_name=display_handler_name, **options)
        elif display_handler_name is None:
            display(content)
        else:
            dh = options["display_handlers"].get(display_handler_name)
            if dh is not None:
                dh.update(content)
            else:
                options["display_handlers"][display_handler_name] = display(content, display_id=options.get("display_id"))


    @staticmethod
    def get_show_deeplink_webbrowser_html_obj(window_name, deep_link_url:str, close_window_timeout_in_secs: int, options=None):
        options = options or {}
        close_itself_timeout_in_secs = 0
        html_str = Display._get_Launch_page_html(window_name, deep_link_url, close_window_timeout_in_secs, close_itself_timeout_in_secs, False, options=options)
        file_name = Display._get_name()
        file_path = Display._html_to_file_path(html_str, file_name, **options)
        url = Display._get_file_path_url(file_path, options=options)
        return url


    @staticmethod
    def get_show_deeplink_html_obj(window_name, deep_link_url:str, close_window_timeout_in_secs: int, options=None):
        options = options or {}
        if options.get("kernel_location") == "local":
            url = Display.get_show_deeplink_webbrowser_html_obj(window_name, deep_link_url, close_window_timeout_in_secs, options=options)
            webbrowser.open(url, new=1, autoraise=True)
            Display.showInfoMessage(f"opened popup window: {window_name}, see your browser")
            return None
        else:
            close_itself_timeout_in_secs = None
            html_str = Display._get_Launch_page_html(window_name, deep_link_url, close_window_timeout_in_secs, close_itself_timeout_in_secs, False, options=options)
            return HTML(html_str)


    @staticmethod
    def get_show_window_html_obj(window_name, file_path, button_text=None, onclick_visibility=None, isText:bool=None, palette:dict=None, before_text=None, after_text=None, close_window_timeout_in_secs=None, content=None, options=None):
        options = options or {}
        html_str = None
        mode = options.get("popup_interaction", "auto")
        if mode == "auto":
            if options.get("notebook_app") in ["ipython"] and options.get("test_notebook_app") in ["none", "ipython"]: 
                mode = "webbrowser_open_at_kernel"
            elif options.get("notebook_app") in ["visualstudiocode", "azuredatastudio"] and options.get("test_notebook_app") in ["none", "visualstudiocode", "azuredatastudio"]: 
                mode = "reference"
            elif options.get("notebook_app") in ["nteract"] and options.get("temp_files_server_address") is None:
                mode = "reference"
            else:
                mode = "button"
        if mode == "webbrowser_open_at_kernel":
            url = Display._get_file_path_url(file_path, options=options)
            # url = urllib.parse.quote(url)
            webbrowser.open(url, new=1, autoraise=True)
            html_str = Display._getInfoMessageHtmlStr(f"opened popup window: '{window_name}', see it in your browser", **options)

        elif mode in ["button", "memory_button"]:
            html_str = Display._get_window_html(window_name, file_path, button_text, onclick_visibility, isText=isText, palette=palette, before_text=before_text, after_text=after_text, close_window_timeout_in_secs=close_window_timeout_in_secs, content=content, options=options)

        elif mode in ["reference", "reference_popup"]:
            html_str = Display._get_window_ref_html(mode == "reference_popup", window_name, file_path, button_text, isText=isText, palette=palette, before_text=before_text, after_text=after_text, close_window_timeout_in_secs=close_window_timeout_in_secs, options=options)

        return HTML(html_str) if html_str is not None else None


    ############################################################################
    #
    ############################################################################
    @staticmethod
    def _get_file_path_url(file_path, options=None):
        options = options or {}
        url = None
        if file_path.startswith("http"):
            url = file_path

        elif options.get("temp_files_server_address") is not None and Display.showfiles_url_base_path.startswith("http"):
            url = f'{Display.showfiles_url_base_path}/{adjust_path_to_uri(file_path)}?kernelid={options.get("kernel_id")}'

        else:
            url = Display._get_file_path_file_url(file_path, options=options)

        return url

    @staticmethod
    def _get_file_path_file_url(file_path, options=None):
        options = options or {}
        url = None
        path_uri = adjust_path_to_uri(f"{Display.showfiles_file_base_path}/{file_path}")
        url = (f"file:///{path_uri}")

        return url


    @staticmethod
    def _get_window_ref_html(isPopupMode, window_name, file_path, ref_text=None, isText=None, palette=None, ref_palette=None, before_text=None, after_text=None, close_window_timeout_in_secs=None, options=None):
        options = options or {}
        url = Display._get_file_path_url(file_path, options=options)
        popup_window_name = window_name

        if isPopupMode or options.get("temp_files_server_address") is not None:
            close_window_timeout_in_secs = close_window_timeout_in_secs or 60  # five minutes
            popup_window_name = f"popup_{window_name}"
            popup_html = Display._get_popup_window_html(url, window_name, close_window_timeout_in_secs, options=options)
            popup_file_name = f"popup_{file_path.split('/')[-1].split('.')[0]}"
            popup_file_path = Display._html_to_file_path(popup_html, popup_file_name, **options)
            url = Display._get_file_path_url(popup_file_path, options=options)

        if options.get("temp_files_server_address") is not None:
            indirect_url = f'{options.get("temp_files_server_address")}/webbrowser?url={urllib.parse.quote(url)}&kernelid={options.get("kernel_id")}'
            url = indirect_url
        ref_text = ref_text or "popup window"
        
        style_div = f"padding: 10px; color: {palette.get('color')}; background-color: {palette.get('background-color')}; border-color: {palette.get('border-color')}" if palette else ""
        ref_palette = ref_palette or Display.ref_style
        style_ref = f"padding: 2px 6px 2px 6px; color: {ref_palette['color']}; background-color: {ref_palette['background-color']}; border-top: {ref_palette['border-top']}; border-right: {ref_palette['border-right']}; border-bottom: {ref_palette['border-bottom']}; border-left: {ref_palette['border-left']}"

        before_text = f"{before_text.replace(' ', '&nbsp;')}&nbsp;" if before_text is not None else ''
        after_text = f"&nbsp;{after_text.replace(' ', '&nbsp;')}" if after_text is not None else ''
        html_str = (
            f"""<!DOCTYPE html>
            <html><body>
            <div style='{style_div}'>
            {before_text}<a href='{url}' style='{style_ref}' target='{popup_window_name}'>{ref_text}</a>{after_text}
            </div>
            </body></html>"""
        )

        return html_str


    @staticmethod
    def show_window(window_name, file_path, button_text=None, onclick_visibility=None, isText:bool=None, palette:dict=None, before_text=None, after_text=None, display_handler_name=None, close_window_timeout_in_secs=None, content=None, **options):
        html_obj = Display.get_show_window_html_obj(window_name, file_path, button_text=button_text, onclick_visibility=onclick_visibility, isText=isText, palette=palette, before_text=before_text, after_text=after_text, close_window_timeout_in_secs=close_window_timeout_in_secs, content=content, options=options)
        if html_obj is not None:
            Display.show_html_obj(html_obj, display_handler_name=display_handler_name, **options)


    @staticmethod
    def to_json_styled_class(item, style=None, options=None):
        options = options or {}
        json_display_style = style or options.get("json_display")
        if json_display_style != "raw" and (isinstance(item, list) or isinstance(item, dict) or type(item).__name__ == "KqlRow"):
            if json_display_style == "formatted":
                return _getitem_FormattedJson(item)
            else:
                return _getitem_JSON(item)
        else:
            return item


    @staticmethod
    def _html_to_file_path(html_str, file_name, **options):
        file_path = f"{Display.showfiles_folder_name}/{file_name}.html"
        full_file_name = adjust_path(f"{Display.showfiles_file_base_path}/{file_path}")
        text_file = open(full_file_name, "wb")
        text_file.write(bytes(html_str, 'utf-8'))
        text_file.close()
        # ipython will delete file at shutdown or by restart
        IPythonAPI.try_add_to_ipython_tempfiles(full_file_name)
        return file_path


    @staticmethod
    def _get_name(**kwargs):
        if kwargs is not None and isinstance(kwargs.get("file_name"), str) and len(kwargs.get("file_name")) > 0:
            name = kwargs.get("file_name")
        else:
            name = uuid.uuid4().hex
        return name


    @staticmethod
    def _get_popup_window_html(url, window_name, close_window_timeout_in_secs, options=None):
        options = options or {}
        window_params = "fullscreen=no,directories=no,location=no,menubar=no,resizable=yes,scrollbars=yes,status=no,titlebar=no,toolbar=no,"
        window_name = window_name.replace(".", "_").replace("-", "_").replace("/", "_").replace(":", "_").replace(" ", "_")

        html_str = (
            f"""<!DOCTYPE html>
            <html><body>
            <script>
            function kql_MagicPopupWindowFunction(url, window_name, window_params, close_window_timeout_in_secs) {{
                window.focus();
                var w = screen.width / 2;
                var h = screen.height / 2;
                var params = 'width=' + w + ',height=' + h;
                var new_window = window.open(url, window_name, window_params + params);
                // close self window after new window is open
                if (new_window != null) {{
                    window.close();
                    close_window_timeout_in_secs = 1;
                }}
                setTimeout(function(){{ window.close(); }}, close_window_timeout_in_secs * 1000);               
            }}

            kql_MagicPopupWindowFunction('{url}','{window_name}','{window_params}',{str(close_window_timeout_in_secs)});
            window.focus();
            </script>
            <p>This page popups <b>'{window_name}'</b> window. It will close itself in few minutes.
            <br><br><br>If <b>'{window_name}'</b> window doesn't popup, popups are probably blocked on this
            page.<br>To enable the popup, you should modify your browser settings to allow popups on pages from this host: http://127.0.0.1:5000.
            To open popup manually press <a href='{url}'>here</a>
            <br><br><br><br><br><br><b>Note:</b> You can disable the popups in your notebook, by setting popup_interaction option 
            to 'reference' (will open in a tab) or 'webbrowser_open_at_kernel' (will auto open in a tab on the python kernel host)
            or 'button'. Some modes are not supported by some jupyter based implementations (try and find out).
            <br> To set the default mode in your notebook, run: %config Kqlmagic.popup_interaction={{mode}}
            <br> To set the mode for the current kql magic execution, add the option -pi '{{mode}}'
            </p>
            </body></html>"""
        )
        return html_str

    @staticmethod
    def _get_window_html(window_name, file_path_or_data, button_text=None, onclick_visibility=None, isText=None, palette=None, before_text=None, after_text=None, close_window_timeout_in_secs=None, close_itself_timeout_in_secs=None, content=None, options=None):
        options = options or {}
        # if isText is True, file_path_or_data is the text
        host_or_text = 'text' if isText else (options.get("notebook_service_address") or "")
        if host_or_text != 'text' and options.get("popup_interaction") == "memory_button":
            if file_path_or_data.startswith("http"):
                with urllib.request.urlopen(file_path_or_data) as bytes_reader:
                    file_path_or_data = bytes_reader.read().decode('utf-8')
            else:
                full_file_name = adjust_path(f"{Display.showfiles_file_base_path}/{file_path_or_data}")
                with open(full_file_name, 'r') as str_reader:
                    # Read the entire file
                    file_path_or_data = str_reader.read()
            file_path_or_data =  base64.b64encode(file_path_or_data.encode('utf-8')).decode('utf-8')
            isText = True
            host_or_text = 'body'
        onclick_visibility = "visible" if onclick_visibility == "visible" else "hidden"
        button_text = button_text or "popup window"
        window_name = window_name.replace(".", "_").replace("-", "_").replace("/", "_").replace(":", "_").replace(" ", "_")
        if window_name[0] in "0123456789":
            window_name = f"w_{window_name}"
        window_params = "fullscreen=no,directories=no,location=no,menubar=no,resizable=yes,scrollbars=yes,status=no,titlebar=no,toolbar=no,"

        style = f"padding: 10px; color: {palette['color']}; background-color: {palette['background-color']}; border-color: {palette['border-color']}" if palette else ""
        before_text = before_text or ''
        after_text = after_text or ''

        close_window_timeout_in_secs = close_window_timeout_in_secs if close_window_timeout_in_secs is not None else -1
        if close_itself_timeout_in_secs is None:
            close_itself_timeout_in_secs =  -1
        elif close_window_timeout_in_secs >= 0:
            close_itself_timeout_in_secs = max(close_itself_timeout_in_secs - close_window_timeout_in_secs, 0)

        if not isText and Display.showfiles_url_base_path.startswith("http"):
            file_path_or_data = Display._get_file_path_url(file_path_or_data, options=options)

            if options.get("temp_files_server_address") is not None and content == "schema" and options.get("notebook_app") in ["nteract"]:
                indirect_url = f'{options.get("temp_files_server_address")}/webbrowser?url={urllib.parse.quote(file_path_or_data)}&kernelid={options.get("kernel_id")}'
                file_path_or_data = indirect_url

            
        html_str = (
            f"""<!DOCTYPE html>
            <html><body>
            <div style='{style}'>
            {before_text}
            <button onclick="this.style.visibility='{onclick_visibility}';
            kql_MagicLaunchWindowFunction({single_quote(file_path_or_data)}, {single_quote(window_params)}, {single_quote(window_name)}, {single_quote(host_or_text)});
            kql_MagicCloseWindow(kql_Magic_{window_name}, {str(close_window_timeout_in_secs)}, {str(close_itself_timeout_in_secs)});">
            {button_text}</button>{after_text}
            </div>

            <script>
            var kql_Magic_{window_name} = null;


            function kql_MagicCloseWindow(window_obj, obj_secs, itself_secs) {{
                if (obj_secs >= 0) {{
                    _timeout = setTimeout(function(){{
                        window_obj.close();
                        if (itself_secs >= 0) {{
                            __timeout = setTimeout(function(){{window.close();}}, itself_secs * 1000);
                        }}
                    }}, obj_secs * 1000);
                }} else if (itself_secs >= 0) {{
                    _timeout = setTimeout(function(){{window.close();}}, itself_secs * 1000);
                }}
            }}

            function kql_MagicLaunchWindowFunction(file_path_or_data, window_params, window_name, host_or_text) {{
                var url;
                const baseURI = String(window.location);
                if (host_or_text == 'text' || host_or_text == 'body') {{
                    url = ''
                }} else if (file_path_or_data.startsWith('http')) {{
                    url = file_path_or_data;
                }} else if (host_or_text.endsWith('.azureml.ms') || host_or_text.endsWith('.azureml.net')) {{
                    let azuremlBaseURI = String(window.document.baseURI);
                    let start = azuremlBaseURI.search('activeFilePath=');
                    if (start > 0) {{
                        start += 'activeFilePath='.length;
                        let end = azuremlBaseURI.substring(start).search('&');
                        if (end < 0) {{
                            end = undefined;
                        }}
                        let parts = azuremlBaseURI.substring(start, end).split('/');
                        parts.pop();
                        url = host_or_text + '/tree/' + parts.join('/') + '/' + file_path_or_data;
                    }} else {{
                        var parts = baseURI.split('/');
                        parts.pop();
                        url = parts.join('/') + '/' + file_path_or_data;
                    }}
                }} else {{
                    var base_url = '';

                    // check if azure notebook
                    var azure_host = (host_or_text == null || host_or_text.length == 0) ? 'https://notebooks.azure.com' : host_or_text;
                    var start = azure_host.search('//');
                    var azure_host_suffix = '.' + azure_host.substring(start+2);

                    var end = baseURI.search(azure_host_suffix);
                    start = baseURI.search('//');
                    if (start > 0 && end > 0) {{
                        // # azure notebook environment, assume template: https://library-user.notebooks.azure.com
                        var parts = baseURI.substring(start+2, end).split('-');
                        if (parts.length == 2) {{
                            var library = parts[0];
                            var user = parts[1];
                            base_url = azure_host + '/api/user/' +user+ '/library/' +library+ '/html/';
                        }}
                    }}

                    // check if local jupyter lab
                    if (base_url.length == 0) {{
                        var configDataScipt  = document.getElementById('jupyter-config-data');
                        if (configDataScipt != null) {{
                            var jupyterConfigData = JSON.parse(configDataScipt.textContent);
                            if (jupyterConfigData['appName'] == 'JupyterLab' && jupyterConfigData['serverRoot'] != null &&  jupyterConfigData['treeUrl'] != null) {{
                                var basePath = {single_quote(Display.showfiles_file_base_path)} + '/';
                                if (basePath.startsWith(jupyterConfigData['serverRoot'])) {{
                                    base_url = '/files/' + basePath.substring(jupyterConfigData['serverRoot'].length+1);
                                }}
                            }}
                        }}
                    }}

                    // works for local jupyter notebook
                    if (base_url.length == 0) {{

                        var parts = baseURI.split('/');
                        parts.pop(); // remove notebook name segment
                        base_url = parts.join('/') + '/';
                    }}
                    url = base_url + file_path_or_data;
                }}

                window.focus();
                var w = screen.width / 2;
                var h = screen.height / 2;
                params = 'width='+w+',height='+h;
                // kql_Magic + window_name should be a global variable 
                window_obj = window.open(url, window_name, window_params + params);
                if (url == '') {{
                    let decodedData = atob(file_path_or_data);
                    if (host_or_text == 'text') {{
                        var el = window_obj.document.createElement('p');
                        window_obj.document.body.overflow = 'auto';
                        el.style.top = 0;
                        el.style.left = 0;
                        el.innerHTML = decodedData;
                        window_obj.document.body.appendChild(el);
                    }} else {{
                        window_obj.document.body.innerHTML = decodedData;
                    }}
                }}
                kql_Magic_{window_name} = window_obj;
            }}
            </script>

            </body></html>"""
        )
        return html_str

    @staticmethod
    def _get_Launch_page_html(window_name, file_path_or_data, close_window_timeout_in_secs, close_itself_timeout_in_secs, isText, options=None):
        options = options or {}
        # if isText is True, file_path_or_data is the text
        host_or_text = 'text' if isText else (options.get("notebook_service_address") or "")
        if host_or_text != 'text' and options.get("popup_interaction") == "memory_button":
            if file_path_or_data.startswith("http"):
                with urllib.request.urlopen(file_path_or_data) as bytes_reader:
                    file_path_or_data = bytes_reader.read().decode('utf-8')
            else:
                full_file_name = adjust_path(f"{Display.showfiles_file_base_path}/{file_path_or_data}")
                with open(full_file_name, 'r') as str_reader:
                    # Read the entire file
                    file_path_or_data = str_reader.read()
            file_path_or_data =  base64.b64encode(file_path_or_data.encode('utf-8')).decode('utf-8')
            isText = True
            host_or_text = 'body'
        window_name = window_name.replace(".", "_").replace("-", "_").replace("/", "_").replace(":", "_").replace(" ", "_")
        if window_name[0] in "0123456789":
            window_name = f"w_{window_name}"
        # negative means not to colose window
        close_window_timeout_in_secs = close_window_timeout_in_secs if close_window_timeout_in_secs is not None else -1
        if close_itself_timeout_in_secs is None:
            close_itself_timeout_in_secs =  -1
        elif close_window_timeout_in_secs >= 0:
            close_itself_timeout_in_secs = max(close_itself_timeout_in_secs - close_window_timeout_in_secs, 0)

        window_params = "fullscreen=no,directories=no,location=no,menubar=no,resizable=yes,scrollbars=yes,status=no,titlebar=no,toolbar=no,"

        html_str = (
            f"""<!DOCTYPE html>
            <html><body>
            <script>
            var kql_Magic_{window_name} = null;
            var kql_Magic_{window_name}_timeout = null;

            function kql_MagicCloseWindow(window_obj, obj_secs, itself_secs) {{
                if (obj_secs >= 0) {{
                    _timeout = setTimeout(function(){{
                        window_obj.close();
                        if (itself_secs >= 0) {{
                            __timeout = setTimeout(function(){{window.close();}}, itself_secs * 1000);
                        }}
                    }}, obj_secs * 1000);
                }} else if (itself_secs >= 0) {{
                    _timeout = setTimeout(function(){{window.close();}}, itself_secs * 1000);
                }}
            }}

            function kql_MagicLaunchWindowFunction(file_path_or_data, window_params, window_name, host_or_text) {{
                var url;
                const baseURI = String(window.location);
                if (host_or_text == 'text' || host_or_text == 'body') {{
                    url = ''
                }} else if (file_path_or_data.startsWith('http')) {{
                    url = file_path_or_data;
                }} else if (host_or_text.endsWith('.azureml.ms') || host_or_text.endsWith('.azureml.net')) {{
                    let azuremlBaseURI = String(window.document.baseURI);
                    let start = azuremlBaseURI.search('activeFilePath=');
                    if (start > 0) {{
                        start += 'activeFilePath='.length;
                        let end = azuremlBaseURI.substring(start).search('&');
                        if (end < 0) {{
                            end = undefined;
                        }}
                        let parts = azuremlBaseURI.substring(start, end).split('/');
                        parts.pop();
                        url = host_or_text + '/tree/' + parts.join('/') + '/' + file_path_or_data;
                    }} else {{
                        var parts = baseURI.split('/');
                        parts.pop();
                        url = parts.join('/') + '/' + file_path_or_data;
                    }}
                }} else {{
                    var base_url = '';

                    // check if azure notebook
                    var azure_host = (host_or_text == null || host_or_text.length == 0) ? 'https://notebooks.azure.com' : host_or_text;
                    var start = azure_host.search('//');
                    var azure_host_suffix = '.' + azure_host.substring(start+2);

                    var end = baseURI.search(azure_host_suffix);
                    start = baseURI.search('//');
                    if (start > 0 && end > 0) {{
                        var parts = baseURI.substring(start+2, end).split('-');
                        if (parts.length == 2) {{
                            var library = parts[0];
                            var user = parts[1];
                            base_url = azure_host + '/api/user/' +user+ '/library/' +library+ '/html/';
                        }}
                    }}

                    // check if local jupyter lab
                    if (base_url.length == 0) {{
                        var configDataScipt  = document.getElementById('jupyter-config-data');
                        if (configDataScipt != null) {{
                            var jupyterConfigData = JSON.parse(configDataScipt.textContent);
                            if (jupyterConfigData['appName'] == 'JupyterLab' && jupyterConfigData['serverRoot'] != null &&  jupyterConfigData['treeUrl'] != null) {{
                                var basePath = {single_quote(Display.showfiles_file_base_path)} + '/';
                                if (basePath.startsWith(jupyterConfigData['serverRoot'])) {{
                                    base_url = '/files/' + basePath.substring(jupyterConfigData['serverRoot'].length+1);
                                }}
                            }}
                        }}
                    }}

                    // works for local jupyter
                    if (base_url.length == 0) {{
                        var parts = baseURI.split('/');
                        parts.pop();
                        base_url = parts.join('/') + '/';
                    }}
                    url = base_url + file_path_or_data;
                }}

                window.focus();
                var w = screen.width / 2;
                var h = screen.height / 2;
                params = 'width='+w+',height='+h;
                // kql_Magic + window_name should be a global variable 
                window_obj = window.open(url, window_name, window_params + params);
                if (url == '') {{
                    let decodedData = atob(file_path_or_data);
                    if (host_or_text == 'text') {{
                        var el = window_obj.document.createElement('p');
                        window_obj.document.body.overflow = 'auto';
                        el.style.top = 0;
                        el.style.left = 0;
                        el.innerHTML = decodedData;
                        window_obj.document.body.appendChild(el);
                    }} else {{
                        window_obj.document.body.innerHTML = decodedData;
                    }}
                }}
                kql_Magic_{window_name} = window_obj;
            }}

            kql_MagicLaunchWindowFunction({single_quote(file_path_or_data)}, {single_quote(window_params)}, {single_quote(window_name)}, {single_quote(host_or_text)});

            kql_MagicCloseWindow(kql_Magic_{window_name}, {str(close_window_timeout_in_secs)}, {str(close_itself_timeout_in_secs)});

            </script>
            </body></html>"""
        )
        return html_str

    @staticmethod
    def toHtml(**kwargs):
        title = '' if kwargs.get('title') is None else f"<title>{Constants.MAGIC_PACKAGE_NAME} - {kwargs.get('title')}</title>"
        return f"""<html>
        <head>
            {kwargs.get('head', '')}
            {title}
        </head>
        <body>
            {kwargs.get('body', '')}
        </body>
        </html>"""


    @staticmethod
    def _getMessageHtml(msg, palette):
        "get query information in as an HTML string"

        style_options = {}
        if isinstance(msg, dict):
            if isinstance(msg.get("style_options"), dict):
                style_options = msg.get("style_options")
                msg = msg.get("message")
        
        if isinstance(msg, list):
            msg_str = "<br>".join(msg)
        elif isinstance(msg, dict):
            msg_str = "<br>".join([f"{k}: {v}" for k, v in msg.items()])
        elif isinstance(msg, str):
            msg_str = msg
        else:
            msg_str = f"{msg or ''}"
        if len(msg_str) > 0:          
            style = {
                "padding": "10px",
                "color": palette["color"],
                "background-color": palette["background-color"],
                "border-color": palette["border-color"],
                **style_options
            }
            style_str = "".join([f"; {k}: {v}" for k,v in style.items()])[1:]
            msg_str = msg_str.replace('"', "&quot;").replace("'", "&apos;").replace("\n", "<br>").replace(" ", "&nbsp;")
            body = f"<div><p style='{style_str}'>{msg_str}</p></div>"
        else:
            body = ""
        return {"body": body}


    @staticmethod
    def getSuccessMessageHtml(msg):
        return Display._getMessageHtml(msg, Display.success_style)


    @staticmethod
    def getInfoMessageHtml(msg):
        return Display._getMessageHtml(msg, Display.info_style)


    @staticmethod
    def getWarningMessageHtml(msg):
        return Display._getMessageHtml(msg, Display.warning_style)


    @staticmethod
    def getDangerMessageHtml(msg):
        return Display._getMessageHtml(msg, Display.danger_style)

    @staticmethod
    def _getMessageHtmlStr(html_msg, display_handler_name=None, **options):
        html_str = None
        if html_msg is None or len(html_msg["body"]) == 0:
            if display_handler_name is not None:
                if options.get("display_id", False):
                    html_str = Display.toHtml(**html_msg)
                # dh = options["display_handlers"].get(display_handler_name)
                # if dh is not None:
                #     html_str = Display.toHtml(**html_msg)
        else:
            html_str = Display.toHtml(**html_msg)

        return html_str


    @staticmethod
    def _showMessage(html_msg, display_handler_name=None, **options):
        html_str = Display._getMessageHtmlStr(html_msg, display_handler_name, **options)

        if html_str is not None:
            Display.show_html(html_str, display_handler_name=display_handler_name, **options)


    @staticmethod
    def _getInfoMessageHtmlStr(msg, display_handler_name=None, **options):
        return Display._getMessageHtmlStr(Display.getInfoMessageHtml(msg), display_handler_name=display_handler_name, **options)


    @staticmethod
    def showSuccessMessage(msg, display_handler_name=None, **options):
        Display._showMessage(Display.getSuccessMessageHtml(msg), display_handler_name=display_handler_name, **options)


    @staticmethod
    def showInfoMessage(msg, display_handler_name=None, **options):
        Display._showMessage(Display.getInfoMessageHtml(msg), display_handler_name=display_handler_name, **options)


    @staticmethod
    def showWarningMessage(msg, display_handler_name=None, **options):
        Display._showMessage(Display.getWarningMessageHtml(msg), display_handler_name=display_handler_name, **options)


    @staticmethod
    def showDangerMessage(msg, display_handler_name=None, **options):
        Display._showMessage(Display.getDangerMessageHtml(msg))

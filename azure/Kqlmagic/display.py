# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

import uuid
import webbrowser
import json
import datetime
import urllib.parse


from IPython.core.display import display, HTML
from IPython.display import JSON
from pygments import highlight
from pygments.lexers.data import JsonLexer
from pygments.formatters.terminal import TerminalFormatter


from .my_utils import adjust_path, adjust_path_to_uri, get_valid_filename_with_spaces


class DateTimeEncoder(json.JSONEncoder):

    def default(self, obj):  # pylint: disable=E0202
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        elif isinstance(obj, datetime.date):
            return obj.isoformat()
        elif isinstance(obj, datetime.timedelta):
            return (datetime.datetime.min + obj).time().isoformat()
        else:
            return super(DateTimeEncoder, self).default(obj)


class FormattedJsonDict(dict):

    def __init__(self, j, *args, **kwargs):
        super(FormattedJsonDict, self).__init__(*args, **kwargs)
        self.update(j)

        formatted_json = json.dumps(self, indent=4, sort_keys=True, cls=DateTimeEncoder)
        self.colorful_json = highlight(formatted_json.encode("UTF-8"), JsonLexer(), TerminalFormatter())


    def get(self, key, default=None):
        item = super(FormattedJsonDict, self).get(key, default)
        return _getitem_FormattedJson(item)


    def __getitem__(self, key):
        return self.get(key)


    def __repr__(self):
        return self.colorful_json


class FormattedJsonList(list):

    def __init__(self, j, *args, **kwargs):
        super(FormattedJsonList, self).__init__(*args, **kwargs)
        self.extend(j)
        formatted_json = json.dumps(self, indent=4, sort_keys=True, cls=DateTimeEncoder)
        self.colorful_json = highlight(formatted_json.encode("UTF-8"), JsonLexer(), TerminalFormatter())


    def __getitem__(self, key):
        item = super(FormattedJsonList, self).__getitem__(key)
        return _getitem_FormattedJson(item)


    def __repr__(self):
        return self.colorful_json


def _getitem_FormattedJson(item):
    if isinstance(item, list):
        return FormattedJsonList(item)
    elif isinstance(item, dict):
        return FormattedJsonDict(item)
    else:
        return item


class Display(object):
    """
    """

    success_style = {"color": "#417e42", "background-color": "#dff0d8", "border-color": "#d7e9c6"}
    danger_style = {"color": "#b94a48", "background-color": "#f2dede", "border-color": "#eed3d7"}
    info_style = {"color": "#3a87ad", "background-color": "#d9edf7", "border-color": "#bce9f1"}
    warning_style = {"color": "#8a6d3b", "background-color": "#fcf8e3", "border-color": "#faebcc"}

    showfiles_base_path = None
    showfiles_folder_name = None
    notebooks_host = None


    @staticmethod
    def show_html(html_str):
        Display.show_html_obj(HTML(html_str))


    @staticmethod
    def show_html_obj(html_obj):
        display(html_obj)


    @staticmethod
    def show(content, **options):
        if isinstance(content, str) and len(content) > 0:
            if options is not None and options.get("popup_window", False):
                file_name = Display._get_name(**options)
                file_path = Display._html_to_file_path(content, file_name, **options)
                Display.show_window(file_name, file_path, **options)
            else:
                Display.show_html(content)
        else:
            display(content)


    @staticmethod
    def get_show_deeplink_html_obj(window_name, deep_link_url:str, isCloseWindow: bool, **options):
            html_str = Display._get_Launch_page_html(window_name, deep_link_url, isCloseWindow, False, **options)
            if options.get("notebook_app") in ["visualstudiocode", "ipython"] and options.get("test_notebook_app") in ["none", "visualstudiocode", "ipython"]:
                file_name = Display._get_name()
                file_path = Display._html_to_file_path(html_str, file_name)
                url = file_path if file_path.startswith("http") else adjust_path_to_uri(f"file:///{Display.showfiles_base_path}/{file_path}") #base path is already adjusted to uri in KqlMagic init
                url = urllib.parse.quote(url)
                webbrowser.open(url, new=1, autoraise=True)
                Display.showInfoMessage(f"opened popup window: {window_name}, see your browser")
                return None
            else:
                return HTML(html_str)


    @staticmethod
    def get_show_window_html_obj(window_name, file_path, button_text=None, onclick_visibility=None,isText:bool=None, palette:dict=None, before_text=None, after_text=None, **options):
        if options.get("notebook_app") in ["visualstudiocode", "ipython"] and options.get("test_notebook_app") in ["none", "visualstudiocode", "ipython"]: 
            url = file_path if file_path.startswith("http") else adjust_path_to_uri(f"file:///{Display.showfiles_base_path}/{file_path}") #base path is already adjusted to uri in KqlMagic init
            url = urllib.parse.quote(url)
            webbrowser.open(url, new=1, autoraise=True)
            Display.showInfoMessage(f"opened popup window: {window_name}, see your browser")
            return None
        else:
            html_str = Display._get_window_html(window_name, file_path, button_text, onclick_visibility, isText=isText, palette=palette, before_text=before_text, after_text=after_text, **options)
            return HTML(html_str)


    @staticmethod
    def show_window(window_name, file_path, button_text=None,onclick_visibility=None, isText:bool=None, palette:dict=None, before_text=None, after_text=None, **options):
        html_obj = Display.get_show_window_html_obj(window_name, file_path, button_text=button_text, onclick_visibility=onclick_visibility, isText=isText, palette=palette, before_text=before_text, after_text=after_text,  **options)
        if html_obj is not None:
            Display.show_html_obj(html_obj)


    @staticmethod
    def to_styled_class(item, **kwargs):
        if kwargs.get("json_display") != "raw" and (isinstance(item, dict) or isinstance(item, list)):
            if kwargs.get("json_display") == "formatted" or kwargs.get("notebook_app") != "jupyterlab":
                return _getitem_FormattedJson(item)
            else:
                return JSON(item)
        else:
            return item


    @staticmethod
    def _html_to_file_path(html_str, file_name, **kwargs):
        file_path = f"{Display.showfiles_folder_name}/{file_name}.html"
        full_file_name = adjust_path(f"{Display.showfiles_base_path}/{file_path}")
        text_file = open(full_file_name, "w")
        text_file.write(html_str)
        text_file.close()
        # ipython will delete file at shutdown or by restart
        ip = get_ipython()  # pylint: disable=undefined-variable
        ip.tempfiles.append(full_file_name)
        return file_path


    @staticmethod
    def _get_name(**kwargs):
        if kwargs is not None and isinstance(kwargs.get("file_name"), str) and len(kwargs.get("file_name")) > 0:
            name = kwargs.get("file_name")
        else:
            name = uuid.uuid4().hex
        return name


    @staticmethod
    def _get_window_html(window_name, file_path, button_text=None, onclick_visibility=None, isText=None, palette=None, before_text=None, after_text=None, **kwargs):
        # if isText is True, file_path is the text
        notebooks_host = 'text' if isText else (Display.notebooks_host or "")
        onclick_visibility = "visible" if onclick_visibility == "visible" else "hidden"
        button_text = button_text or "popup window"
        window_name = window_name.replace(".", "_").replace("-", "_").replace("/", "_").replace(":", "_").replace(" ", "_")
        if window_name[0] in "0123456789":
            window_name = "w_" + window_name
        window_params = "fullscreen=no,directories=no,location=no,menubar=no,resizable=yes,scrollbars=yes,status=no,titlebar=no,toolbar=no,"

        style = f"padding: 10px; color: {palette['color']}; background-color: {palette['background-color']}; border-color: {palette['border-color']}" if palette else ""
        before_text = before_text or ''
        after_text = after_text or ''

        html_str = (
            """<!DOCTYPE html>
            <html><body>
            <div style='""" + style + """'>
            """ + before_text + """

            <button onclick="this.style.visibility='"""
            + onclick_visibility
            + """';kql_MagicLaunchWindowFunction('"""
            + file_path
            + """','"""
            + window_params
            + """','"""
            + window_name
            + """','"""
            + notebooks_host
            + """')">"""
            + button_text
            + """</button>
            """ + after_text + """
            </div>

            <script>

            function kql_MagicLaunchWindowFunction(file_path, window_params, window_name, notebooks_host) {
                var url;
                if (notebooks_host == 'text') {
                    url = ''
                } else if (file_path.startsWith('http')) {
                    url = file_path;
                } else {
                    var base_url = '';

                    // check if azure notebook
                    var azure_host = (notebooks_host == null || notebooks_host.length == 0) ? 'https://notebooks.azure.com' : notebooks_host;
                    var start = azure_host.search('//');
                    var azure_host_suffix = '.' + azure_host.substring(start+2);

                    var loc = String(window.location);
                    var end = loc.search(azure_host_suffix);
                    start = loc.search('//');
                    if (start > 0 && end > 0) {
                        var parts = loc.substring(start+2, end).split('-');
                        if (parts.length == 2) {
                            var library = parts[0];
                            var user = parts[1];
                            base_url = azure_host + '/api/user/' +user+ '/library/' +library+ '/html/';
                        }
                    }

                    // check if local jupyter lab
                    if (base_url.length == 0) {
                        var configDataScipt  = document.getElementById('jupyter-config-data');
                        if (configDataScipt != null) {
                            var jupyterConfigData = JSON.parse(configDataScipt.textContent);
                            if (jupyterConfigData['appName'] == 'JupyterLab' && jupyterConfigData['serverRoot'] != null &&  jupyterConfigData['treeUrl'] != null) {
                                var basePath = '""" + Display.showfiles_base_path + """' + '/';
                                if (basePath.startsWith(jupyterConfigData['serverRoot'])) {
                                    base_url = '/files/' + basePath.substring(jupyterConfigData['serverRoot'].length+1);
                                }
                            } 
                        }
                    }

                    // assume local jupyter notebook
                    if (base_url.length == 0) {

                        var parts = loc.split('/');
                        parts.pop();
                        base_url = parts.join('/') + '/';
                    }
                    url = base_url + file_path;
                }

                window.focus();
                var w = screen.width / 2;
                var h = screen.height / 2;
                params = 'width='+w+',height='+h;
                kql_Magic_""" + window_name + """ = window.open(url, window_name, window_params + params);
                if (url == '') {
                    var el = kql_Magic_""" + window_name + """.document.createElement('p');
                    kql_Magic_""" + window_name + """.document.body.overflow = 'auto';
                    el.style.top = 0;
                    el.style.left = 0;
                    el.innerHTML = file_path;
                    kql_Magic_""" + window_name + """.document.body.appendChild(el);
                }
            }
            </script>

            </body></html>"""
        )
        # print(html_str)
        return html_str

    @staticmethod
    def _get_Launch_page_html(window_name, file_path, isCloseWindow, isText, **kwargs):
        # if isText is True, file_path is the text
        notebooks_host = 'text' if isText else (Display.notebooks_host or "")
        window_name = window_name.replace(".", "_").replace("-", "_").replace("/", "_").replace(":", "_").replace(" ", "_")
        if window_name[0] in "0123456789":
            window_name = "w_" + window_name
        close_window_sleep = '5000' if isCloseWindow else '0'
        window_params = "fullscreen=no,directories=no,location=no,menubar=no,resizable=yes,scrollbars=yes,status=no,titlebar=no,toolbar=no,"

        html_str = (
            """<!DOCTYPE html>
            <html><body>
            <script>

            function kql_MagicSleep(ms) {
                return new Promise(resolve => setTimeout(resolve, ms));
            }

            async function kql_MagicCloseWindow(window_obj, ms) {
                if (ms > 0) {
                    await kql_MagicSleep(ms);
                    window_obj.close();
                }
            }

            function kql_MagicLaunchWindowFunction(file_path, window_params, window_name, notebooks_host) {
                var url;
                if (notebooks_host == 'text') {
                    url = ''
                } else if (file_path.startsWith('http')) {
                    url = file_path;
                } else {
                    var base_url = '';

                    // check if azure notebook
                    var azure_host = (notebooks_host == null || notebooks_host.length == 0) ? 'https://notebooks.azure.com' : notebooks_host;
                    var start = azure_host.search('//');
                    var azure_host_suffix = '.' + azure_host.substring(start+2);

                    var loc = String(window.location);
                    var end = loc.search(azure_host_suffix);
                    start = loc.search('//');
                    if (start > 0 && end > 0) {
                        var parts = loc.substring(start+2, end).split('-');
                        if (parts.length == 2) {
                            var library = parts[0];
                            var user = parts[1];
                            base_url = azure_host + '/api/user/' +user+ '/library/' +library+ '/html/';
                        }
                    }

                    // check if local jupyter lab
                    if (base_url.length == 0) {
                        var configDataScipt  = document.getElementById('jupyter-config-data');
                        if (configDataScipt != null) {
                            var jupyterConfigData = JSON.parse(configDataScipt.textContent);
                            if (jupyterConfigData['appName'] == 'JupyterLab' && jupyterConfigData['serverRoot'] != null &&  jupyterConfigData['treeUrl'] != null) {
                                var basePath = '""" + Display.showfiles_base_path + """' + '/';
                                if (basePath.startsWith(jupyterConfigData['serverRoot'])) {
                                    base_url = '/files/' + basePath.substring(jupyterConfigData['serverRoot'].length+1);
                                }
                            } 
                        }
                    }

                    // assume local jupyter notebook
                    if (base_url.length == 0) {

                        var parts = loc.split('/');
                        parts.pop();
                        base_url = parts.join('/') + '/';
                    }
                    url = base_url + file_path;
                }

                window.focus();
                var w = screen.width / 2;
                var h = screen.height / 2;
                params = 'width='+w+',height='+h;
                kql_Magic_""" + window_name + """ = window.open(url, window_name, window_params + params);
                if (url == '') {
                    var el = kql_Magic_""" + window_name + """.document.createElement('p');
                    kql_Magic_""" + window_name + """.document.body.overflow = 'auto';
                    el.style.top = 0;
                    el.style.left = 0;
                    el.innerHTML = file_path;
                    kql_Magic_""" + window_name + """.document.body.appendChild(el);
                }
            }

            kql_MagicLaunchWindowFunction(
                '""" + file_path + """',
                '""" + window_params + """',
                '""" + window_name + """',
                '""" + notebooks_host + """'
            );

            kql_MagicCloseWindow(
                kql_Magic_""" + window_name + """,
                """ + close_window_sleep + """
            );

            </script>
            </body></html>"""
        )
        # print(html_str)
        return html_str

    @staticmethod
    def toHtml(**kwargs):
        return f"""<html>
        <head>
        {kwargs.get('head', '')}
        </head>
        <body>
        {kwargs.get('body', '')}
        </body>
        </html>"""


    @staticmethod
    def _getMessageHtml(msg, palette):
        "get query information in as an HTML string"
        if isinstance(msg, list):
            msg_str = "<br>".join(msg)
        elif isinstance(msg, str):
            msg_str = msg
        else:
            msg_str = str(msg)
        if len(msg_str) > 0:
            # success_style
            msg_str = msg_str.replace('"', "&quot;").replace("'", "&apos;").replace("\n", "<br>").replace(" ", "&nbsp;")
            body = "<div><p style='padding: 10px; color: {0}; background-color: {1}; border-color: {2}'>{3}</p></div>".format(
                palette["color"], palette["background-color"], palette["border-color"], msg_str
            )
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
    def _showMessage(html_msg, **kwargs):
        html_str = Display.toHtml(**html_msg)
        Display.show_html(html_str)


    @staticmethod
    def showSuccessMessage(msg, **options):
        Display._showMessage(Display.getSuccessMessageHtml(msg))


    @staticmethod
    def showInfoMessage(msg, **options):
        Display._showMessage(Display.getInfoMessageHtml(msg))


    @staticmethod
    def showWarningMessage(msg, **options):
        Display._showMessage(Display.getWarningMessageHtml(msg))


    @staticmethod
    def showDangerMessage(msg, **options):
        Display._showMessage(Display.getDangerMessageHtml(msg))


import uuid
from IPython.core.display import display, HTML
from IPython.core.magics.display import Javascript





class Display(object):
    """
    """
    success_style = {'color': '#417e42', 'background-color': '#dff0d8', 'border-color': '#d7e9c6' }
    danger_style = {'color': '#b94a48', 'background-color': '#f2dede', 'border-color': '#eed3d7' }
    info_style = {'color': '#3a87ad', 'background-color': '#d9edf7', 'border-color': '#bce9f1' }
    warning_style = {'color': '#8a6d3b', 'background-color': '#fcf8e3', 'border-color': '#faebcc' }
    showfiles_base_url = None
    showfiles_base_path = None

    @staticmethod
    def show(html_str, **kwargs):
        if len(html_str) > 0:
            if kwargs is not None and kwargs.get('popup_window', False):
                file_name = Display._get_name(**kwargs)
                url = Display._html_to_url(html_str, file_name, **kwargs)
                Display.show_window(file_name, url, kwargs.get('botton_text'), **kwargs)
            else:
                # print(HTML(html_str)._repr_html_())
                display(HTML(html_str))

    @staticmethod
    def show_window(window_name, url, button_text = None, **kwargs):
        html_str = Display._get_window_html(window_name, url, button_text, **kwargs)
        display(HTML(html_str))

    @staticmethod
    def show_windows(windows, **kwargs):
        # display(Javascript(script))
        html_str = Display._get_windows_html(windows, **kwargs)
        display(HTML(html_str))

    @staticmethod
    def _html_to_url(html_str, file_name, **kwargs):
        fname = Display.showfiles_base_path +file_name+ ".html"
        text_file = open(fname, "w")
        text_file.write(html_str)
        text_file.close()
        get_ipython().tempfiles.append(fname)

        return Display.showfiles_base_url +file_name+ '.html' 

    @staticmethod
    def _get_name(**kwargs):
        if kwargs is not None and isinstance(kwargs.get('name'), str) and len(kwargs.get('name')) > 0:
            name = kwargs.get('name')
        else:
            name = uuid.uuid4().hex
        return name

    @staticmethod
    def _get_windows_html(windows, **kwargs):
        windowFunctionName = "kqlMagicLaunchWindowFunction"
        if kwargs is not None and kwargs.get('windowFunctionName'):
            windowFunctionName = kwargs.get('windowFunctionName')

        html_part1 = """<!DOCTYPE html>
            <html>
            <body>

            <button onclick="this.style.visibility='hidden';"""+windowFunctionName+"""Function()">popup window</button>

            <script>

            function """+windowFunctionName+"""Function() {
                var w = screen.width / 2;
                var h = screen.height / 2;
                params = 'width='+w+',height='+h;
            """

        html_part3 = """
            }
            </script>

            </body>
            </html>"""
        html_part2 = ''
        for window_name in windows.keys():
            url = windows.get(window_name)
            window_params = "fullscreen=no,directories=no,location=no,menubar=no,resizable=yes,scrollbars=yes,status=no,titlebar=no,toolbar=no,"
            html_part2 += 'kqlMagic_' +window_name+ """ = window.open('""" +url+ """', '""" +window_name+ """', '""" +window_params+ """'+params);"""
        result =  html_part1 + html_part2 + html_part3
        # print(result)
        return result

    @staticmethod
    def _get_window_html(window_name, url, button_text = None, **kwargs):
        button_text = button_text or 'popup window'
        window_params = "fullscreen=no,directories=no,location=no,menubar=no,resizable=yes,scrollbars=yes,status=no,titlebar=no,toolbar=no,"
        html_str = """<!DOCTYPE html>
            <html><body>

            <button onclick="this.style.visibility='hidden';kqlMagicLaunchWindowFunction('"""+url+"""','""" +window_params+ """','""" +window_name+ """')">""" +button_text+ """</button>

            <script>
            function kqlMagicLaunchWindowFunction(url, window_params, window_name) {
                window.focus();
                var w = screen.width / 2;
                var h = screen.height / 2;
                params = 'width='+w+',height='+h;
                kqlMagic_""" +window_name+ """ = window.open(url, window_name, window_params + params);
            }
            </script>

            </body></html>"""
        # print(html_str)
        return html_str

    @staticmethod
    def toHtml(**kwargs):
        return """<html>
        <head>
        {0}
        </head>
        <body>
        {1}
        </body>
        </html>""".format(kwargs.get("head", ""), kwargs.get("body", ""))

    @staticmethod
    def _getMessageHtml(msg, palette):
        "get query information in as an HTML string"
        if isinstance(msg, list):
            msg_str = '<br>'.join(msg)
        elif isinstance(msg, str):
            msg_str = msg
        else:
            msg_str = str(msg)
        if len(msg_str) > 0:
            # success_style
            msg_str = msg_str.replace('"', '&quot;').replace("'", '&apos;').replace('\n', '<br>').replace(' ', '&nbsp')
            body =  "<div><p style='padding: 10px; color: {0}; background-color: {1}; border-color: {2}'>{3}</p></div>".format(
                palette['color'], palette['background-color'], palette['border-color'], msg_str)
        else:
           body = ""
        return {"body" : body}


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
    def showSuccessMessage(msg, **kwargs):
        html_str = Display.toHtml(**Display.getSuccessMessageHtml(msg))
        Display.show(html_str, **kwargs)

    @staticmethod
    def showInfoMessage(msg, **kwargs):
        html_str = Display.toHtml(**Display.getInfoMessageHtml(msg))
        Display.show(html_str, **kwargs)
            
    @staticmethod
    def showWarningMessage(msg, **kwargs):
        html_str = Display.toHtml(**Display.getWarningMessageHtml(msg))
        Display.show(html_str, **kwargs)

    @staticmethod
    def showDangerMessage(msg, **kwargs):
        html_str = Display.toHtml(**Display.getDangerMessageHtml(msg))
        Display.show(html_str, **kwargs)

import time
from IPython.core.display import display
from IPython.core.magics.display import Javascript


class Help_html(object):
    """
    """

    notebooks_host = None
    showfiles_base_url = None
    _pending_helps = {}

    @staticmethod
    def flush(window_location, **kwargs):
        if window_location.startswith("http://localhost") or window_location.startswith("https://localhost"):
            start = window_location[8:].find("/") + 9
            parts = window_location[start:].split("/")
            parts.pop()
            # Help_html.showfiles_base_url = window_location[:start] + 'files/' + '/'.join(parts)
            Help_html.showfiles_base_url = window_location[:start] + "/".join(parts)
        else:
            if Help_html.notebooks_host:
                start = Help_html.notebooks_host.find("//") + 2
                suffix = "." + Help_html.notebooks_host[start:]
            else:
                suffix = ".notebooks.azure.com"
            end = window_location.find(suffix)
            start = window_location.find("//")
            # azure notebook environment, assume template: https://library-user.libray.notebooks.azure.com
            if start > 0 and end > 0:
                library, user = window_location[start + 2 : end].split("-")
                azure_notebooks_host = Help_html.notebooks_host or "https://notebooks.azure.com"
                Help_html.showfiles_base_url = azure_notebooks_host + "/api/user/" + user + "/library/" + library + "/html"
            # assume just a remote kernel, as local
            else:
                parts = window_location.split("/")
                parts.pop()
                Help_html.showfiles_base_url = "/".join(parts)

        refresh = False
        for text, url in Help_html._pending_helps.items():
            Help_html.add_menu_item(text, url, False, **kwargs)
            refresh = True
        Help_html._pending_helps = {}
        if refresh:
            Help_html._reconnect(**kwargs)

    @staticmethod
    def add_menu_item(text, file_path: str, reconnect=True, **kwargs):
        if not text:
            return

        if not file_path:
            return

        # add help link
        if file_path.startswith("http"):
            url = file_path
        elif Help_html.showfiles_base_url is not None:
            url = Help_html.showfiles_base_url + "/" + file_path
        else:
            url = None

        if url:
            help_links = get_ipython().kernel._trait_values["help_links"]
            found = False
            for link in help_links:
                # if found update url
                if link.get("text") == text:
                    if link.get("url") != url:
                        link["url"] = url
                    else:
                        reconnect = False
                    found = True
                    break
            if not found:
                help_links.append({"text": text, "url": url})
            # print('help_links: ' + str(help_links))
            if reconnect:
                Help_html._reconnect(**kwargs)
        elif Help_html._pending_helps.get(text) is None:
            Help_html._pending_helps[text] = file_path

    @staticmethod
    def _reconnect(**kwargs):
        if kwargs is None or kwargs.get("notebook_app") != "jupyterlab":
            display(Javascript("""IPython.notebook.kernel.reconnect();"""))
            time.sleep(1)

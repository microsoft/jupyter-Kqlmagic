# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------


from .display import Display

class Help_html(object):
    """
    """

    notebooks_host = None
    showfiles_base_url = None
    _pending_helps = {}


    @staticmethod
    def flush(window_location, **options):
        if window_location.startswith("http://localhost") or window_location.startswith("https://localhost"):
            start = window_location[8:].find("/") + 9
            parts = window_location[start:].split("/")
            parts.pop()
            Help_html.showfiles_base_url = window_location[:start] + "/".join(parts)
        else:
            if Help_html.notebooks_host:
                host = Help_html.notebooks_host or ""
                start = host.find("//") + 2
                suffix = "." + host[start:]
            else:
                suffix = ".notebooks.azure.com"
            end = window_location.find(suffix)
            start = window_location.find("//")
            # azure notebook environment, assume template: https://library-user.libray.notebooks.azure.com
            if start > 0 and end > 0 and ('-' in window_location):
                library, user = window_location[start + 2 : end].split("-", 1)
                azure_notebooks_host = Help_html.notebooks_host or "https://notebooks.azure.com"
                Help_html.showfiles_base_url = f"{azure_notebooks_host}/api/user/{user} /library/{library}/html"
            # assume just a remote kernel, as local
            else:
                parts = window_location.split("/")
                parts.pop()
                Help_html.showfiles_base_url = "/".join(parts)

        refresh = False
        for text, url in Help_html._pending_helps.items():
            Help_html.add_menu_item(text, url, False, **options)
            refresh = True
        Help_html._pending_helps = {}
        if refresh:
            Display.kernelReconnect(**options)


    @staticmethod
    def add_menu_item(text, file_path: str, reconnect=True, **options):
        if not text:
            return

        if not file_path:
            return

        # add help link
        if file_path.startswith("http"):
            url = file_path
        elif Help_html.showfiles_base_url is not None:
            url = f"{Help_html.showfiles_base_url}/{file_path}"
        else:
            url = None

        if url:
            Display.add_to_help_links(text, url, reconnect, **options)
        elif Help_html._pending_helps.get(text) is None:
            Help_html._pending_helps[text] = file_path

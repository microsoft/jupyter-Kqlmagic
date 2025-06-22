# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------


from .ipython_api import IPythonAPI


class Help_html(object):
    """
    adds entries to jupyter help
    """

    showfiles_base_url = None
    _pending_helps = {}


    @staticmethod
    def flush(window_location:str, options:dict=None):
        options = options or {}
        base_url = None
        # local machine jupyter
        if window_location.startswith(("http://localhost", "https://localhost", "http://127.0.0.", "https://127.0.0.")):           
            # example: http://localhost:8888/notebooks/my%20notebooks/legend.ipynb
            parts = window_location.split("/")
            parts.pop()  # remove notebook name 
            del parts[3]  # remove '/notebooks'
            base_url = "/".join(parts)

        # know remote service address
        else:
            notebook_service_address = options.get("notebook_service_address")
            if notebook_service_address is not None:
                # azure notebooks service
                if notebook_service_address.endswith("notebooks.azure.com"):
                    start = notebook_service_address.find("//") + 2
                    host_suffix = "." + notebook_service_address[start:]

                    start = window_location.find("//")
                    end = window_location.find(host_suffix)
                    # azure notebook environment, assume template: https://library-user.notebooks.azure.com
                    if start > 0 and end > 0 and ('-' in window_location):
                        library_user_segment = window_location[start + 2: end]
                        if '-' in library_user_segment:
                            library, user = library_user_segment.split("-", 1)
                            base_url = f"{notebook_service_address}/api/user/{user}/library/{library}/html"

                # other remote services (assume support /tree)
                if base_url is None:
                    parts = notebook_service_address.split("/")
                    base_url = "/".join(parts[:3])  + '/tree'
                
            # default (assume support /tree)
            if base_url is None:
                # assume a remote kernel url pattern  as local
                parts = window_location.split("/")
                base_url = "/".join(parts[:3])  + '/tree'

        Help_html.showfiles_base_url = base_url
        refresh = False
        for text, url in Help_html._pending_helps.items():
            Help_html.add_menu_item(text, url, False, **options)
            refresh = True
        Help_html._pending_helps = {}
        if refresh:
            IPythonAPI.try_kernel_reconnect(**options)


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
            IPythonAPI.try_add_to_help_links(text, url, reconnect, **options)
        elif Help_html._pending_helps.get(text) is None:
            Help_html._pending_helps[text] = file_path

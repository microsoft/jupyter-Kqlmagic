# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------


import sys
import os
import subprocess as sub


import requests


from .log import logger
from .constants import Constants
from .ipython_api import IPythonAPI
from .my_utils import quote_spaced_items_in_path


DEFAULT_PROTOCOL = "http"
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = "5000"


class FilesServerManagement(object):

    def __init__(self, server_py_code, server_url, base_folder, folders, options={}):
        protocol, host, port = self.parse_server_url(server_url)
        self._server_py_code = server_py_code
        self._protocol = protocol or DEFAULT_PROTOCOL
        self._host = host or DEFAULT_HOST
        self._port = port or self.get_notused_port(host=self._host) or DEFAULT_PORT
        self._base_folder = base_folder
        self._folders = folders # comma separated folders
        self._server_url = f"{self._protocol}://{self._host}:{self._port}"
        self._is_registered = False
        self._is_started = False
        self._kernel_id = options.get("kernel_id")
        logger().debug(f"FilesServerManagement::startServer init: server_py_code: {server_py_code}, server_url: {self._server_url}, base_folder: {base_folder}, folders: {folders}, _kernel_id: {self._kernel_id}")


    def get_notused_port(self, host=""):
        try:
            import socket
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.bind((host,0))
            s.listen(1)
            port = s.getsockname()[1]
            s.close()
            return port
        except:
            pass


    def parse_server_url(self, url: str):
        protocol = host = port = None
        if url is not None:
            url = url.lower()
            parts = url.split("://", 1)
            if len(parts) == 2:
                protocol = parts[0]
                url = parts[1]
            parts = url.split(":", 1)
            if len(parts) == 2:
                host = parts[0] if len(parts[0]) > 0 else None
                port = parts[1] if len(parts[1]) > 0 else None
            else:
                try:
                    port = int(parts[0])
                except:
                    host = parts[0]
        return protocol, host, port


    @property
    def files_url(self) -> str:
        return f"{self._server_url}/files"


    @property
    def folders_url(self) -> str:
        return f"{self._server_url}/folders"


    @property
    def server_url(self) -> str:
        return self._server_url


    def startServer(self):
        logger().debug(f"FilesServerManagement::startServer")
        if not self._is_started or not self.pingServer():
            python_exe = sys.executable or 'python'
            os.environ['FLASK_ENV'] = "development"
            window_visibility = os.getenv(f'{Constants.MAGIC_CLASS_NAME_UPPER}_FILES_SERVER_WINDOW_VISIBILITY')
            show_window = window_visibility is not None and window_visibility.lower() == "show"
            logger().debug(f"FilesServerManagement::startServer with show_window: {show_window}")

            # must use double quotes for base folder and folders, to allow spaces (note single quoted does not work)
            command_params = f'-protocol={self._protocol} -host={self._host} -port={self._port} -base_folder="{self._base_folder}" -folders="{self._folders}" -parent_id="{os.getpid()}" -parent_kernel_id="{self._kernel_id}" -clean="folders"'
            command_head = "start /min /wait" if show_window else ""
            python_exe = quote_spaced_items_in_path(python_exe)
            command = f'{command_head} {python_exe} "{self._server_py_code}" {command_params}'
            sub.Popen(command, shell=True)
            logger().debug(f"FilesServerManagement::startServer start sub process command: {command}")

            self._is_started = True

        if not self._is_registered:
            IPythonAPI.try_register_to_ipython_atexit(FilesServerManagement._abortServer, self._server_url, self._kernel_id)
            self._is_registered = True
            logger().debug(f"FilesServerManagement::startServer try_register_to_ipython_atexit: _abortServer({self._server_url}, {self._kernel_id})")


    def pingServer(self):
        try:
            ping_url = f'{self._server_url}/ping?kernelid={self._kernel_id}'
            requests.get(ping_url)
            result = True
        except:
            result = False
        logger().debug(f"FilesServerManagement::startServer pingServer: url: {ping_url}, result: {result}")
        return result


    def abortServer(self):
        FilesServerManagement._abortServer(self._server_url, self._kernel_id)


    @staticmethod
    def _abortServer(server_url, kernel_id):
        try:
            abort_url = f'{server_url}/abort?kernelid={kernel_id}'
            requests.get(abort_url)
            result = True
        except:
            result = False
        logger().debug(f"FilesServerManagement::_abortServer abortServer: url: {abort_url}, result: {result}")

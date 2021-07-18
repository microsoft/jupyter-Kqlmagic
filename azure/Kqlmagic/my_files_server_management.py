# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------


import sys
import os
import time
import subprocess as sub
import threading
from typing import Tuple


from .dependencies import Dependencies
from .log import logger
from .constants import Constants
from .my_utils import get_env_var, double_quote
from .ipython_api import IPythonAPI
from .my_utils import quote_spaced_items_in_path


DEFAULT_PROTOCOL = "http"
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = "5000"


class FilesServerManagement(object):

    def __init__(self, server_py_code_path, server_url, base_folder, folders, options={}):
        protocol, host, port = self.parse_server_url(server_url)
        self._server_py_code_path = server_py_code_path
        self._protocol = protocol or DEFAULT_PROTOCOL
        self._host = host or DEFAULT_HOST
        self._port = port or self.get_notused_port(host=self._host) or DEFAULT_PORT
        self._base_folder = base_folder
        self._folders = folders  # comma separated folders
        self._server_url = f"{self._protocol}://{self._host}:{self._port}"
        self._is_registered = False
        self._is_started = False
        self._heartbeat_thread = None
        self._kernel_id = options.get("kernel_id")
        self._liveness_mode = self._find_liveness_mode(sys.platform)
        self._pid = os.getpid()
        logger().debug(
            f"FilesServerManagement::startServer init: " 
            f"server_py_code_path: {server_py_code_path}, server_url: {self._server_url}, base_folder: {base_folder}, folders: {folders}, _kernel_id: {self._kernel_id}, liveness_mode: {self._liveness_mode}")


    def get_notused_port(self, host:str="")->str:
        try:
            import socket
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.bind((host,0))
            s.listen(1)
            port = s.getsockname()[1]
            s.close()
            return str(port)
        except:
            pass


    def parse_server_url(self, url:str)->Tuple[str, str, str]:
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
                    port = str(int(parts[0]))
                except:
                    host = parts[0]
        return protocol, host, port


    @property
    def files_url(self)->str:
        return f"{self._server_url}/files"


    @property
    def folders_url(self)->str:
        return f"{self._server_url}/folders"


    @property
    def server_url(self)->str:
        return self._server_url




    def startServer(self)->None:
        logger().debug(f"FilesServerManagement::startServer")
        if not self._is_started or not self.pingServer():
            python_exe = sys.executable or 'python'
            os.environ['FLASK_ENV'] = "development"
            window_visibility = get_env_var(f'{Constants.MAGIC_CLASS_NAME_UPPER}_FILES_SERVER_WINDOW_VISIBILITY')
            show_window = window_visibility is not None and window_visibility.lower() == "show"
            show_window = show_window or self._liveness_mode == "show"
            logger().debug(f"FilesServerManagement::startServer with show_window: {show_window}")

            # must use double quotes for base folder and folders, to allow spaces (note single quoted does not work)
            command_params = (
                f'-protocol={self._protocol} -host={self._host} -port={self._port} '
                f'-base_folder={double_quote(self._base_folder)} -folders={double_quote(self._folders)} '
                f'-parent_id={double_quote(self._pid)} -parent_kernel_id={double_quote(self._kernel_id)} '
                f'-liveness_mode={double_quote(self._liveness_mode)} -clean="folders"')
            command_head = "start /min /wait" if show_window else ""
            python_exe = quote_spaced_items_in_path(python_exe)
            command = f'{command_head} {python_exe} {double_quote(self._server_py_code_path)} {command_params}'
            sub.Popen(command, shell=True)
            logger().debug(f"FilesServerManagement::startServer start sub process command: {command}")

            self._is_started = True

            if self._liveness_mode == "heartbeat":
                # alternative mechanism to detect parent is still alive
                # parent is send heartbeat, and if for a period of time heartbeat stops, mean parent exited
                self._heartbeat_thread = HeartbeatThread(self)
                self._heartbeat_thread.start()

        if not self._is_registered:
            IPythonAPI.try_register_to_ipython_atexit(AbortFileServer.abortServer, self._server_url, self._kernel_id, self._heartbeat_thread)
            self._is_registered = True
            logger().debug(f"FilesServerManagement::startServer try_register_to_ipython_atexit: _abortServer({self._server_url}, {self._kernel_id})")


    def _find_liveness_mode(self, platform:str)->str:
        psutil = Dependencies.get_module("psutil")
        requests = Dependencies.get_module("requests")
        liveness_mode = "show"
        if platform == 'win32':
            if psutil:
                liveness_mode = "parent_process_state"
            elif requests:
                liveness_mode = "heartbeat"
        else:
            if self._pid != 1:
                liveness_mode = "parent_process_id_value"
            elif requests:
                liveness_mode = "heartbeat"
            elif psutil:
                liveness_mode = "parent_process_state"

        return liveness_mode


    def pingServer(self)->bool:
        ping_url = None
        result = False
        try:
            ping_url = f'{self._server_url}/ping?kernelid={self._kernel_id}'
            requests = Dependencies.get_module("requests", dont_throw=True)
            if requests:
                requests.get(ping_url)
            result = True
        except:
            pass

        logger().debug(f"FilesServerManagement::pingServer: url: {ping_url}, result: {result}")
        return result


    def heartbeat(self)->bool:
        heartbeat_url = None
        result = False
        try:
            heartbeat_url = f"{self._server_url}/heartbeat?kernelid={self._kernel_id}"
            requests = Dependencies.get_module("requests", dont_throw=True)
            if requests:
                requests.get(heartbeat_url)
                result = True
        except:
            pass

        logger().debug(f"FilesServerManagement::heartbeat: url: {heartbeat_url}, result: {result}")
        return result


    def abortServer(self)->None:
        AbortFileServer.abortServer(self._server_url, self._kernel_id, self._heartbeat_thread)



class HeartbeatThread(threading.Thread):

    def __init__(self, filesServerManagement:FilesServerManagement)->None:
        super(HeartbeatThread, self).__init__()

        self.daemon = True
        self._files_server_management = filesServerManagement
        self._stop_event = threading.Event()


    def run(self)->None:
        while True:
            try:
                if self.stopped():
                    logger().debug(f"HeartbeatThread::HeartbeatThread: exit")
                    break
                self._files_server_management.heartbeat()
            except:
                pass
            time.sleep(1.0)


    def stop(self)->None:
        self._stop_event.set()


    def stopped(self)->bool:
        return self._stop_event.is_set()


class AbortFileServer(object):
    
    @staticmethod
    def abortServer(server_url:str, kernel_id:str, heartbeat_thread:HeartbeatThread)->None:
        abort_url = None
        result = False
        try:
            if heartbeat_thread is not None:
                try:
                    heartbeat_thread.stop()
                except:
                    pass
            abort_url = f"{server_url}/abort?kernelid={kernel_id}"
            requests = Dependencies.get_module("requests", dont_throw=True)
            if requests:
                requests.get(abort_url)
                result = True
        except:
            pass

        logger().debug(f"FilesServerManagement::_abortServer: url: {abort_url}, result: {result}")

        if heartbeat_thread is not None:
            try:
                heartbeat_thread.join()
            except:
                pass

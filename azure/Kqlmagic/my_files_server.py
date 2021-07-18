# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

from typing import Union
import sys
import os
from datetime import datetime
import time
from threading import Thread, Event
from _thread import interrupt_main
import logging
from logging import Logger
import errno


from flask import Flask, send_file, request, make_response, Response


MAGIC_CLASS_NAME_UPPER = "KQLMAGIC"


def _get_env_var(var_name:str)->str:
    value = os.getenv(var_name)
    if value:
        # value = value.strip().upper().replace("_", "").replace("-", "")
        if value.startswith(("'", '"')):
            value = value[1:-1].strip()
    return value


logger:Logger = None


def init_logger(kernel_id:str, log_level:str=None, log_file:str=None, log_file_prefix:str=None, log_file_mode:str=None)->Logger:
    global logger
    # create logger
    logger = logging.getLogger("kqlmagic-srv")
    logger.setLevel(logging.DEBUG)

    log_level = log_level or _get_env_var(f"{MAGIC_CLASS_NAME_UPPER}_LOG_LEVEL")
    log_file = log_file or _get_env_var(f"{MAGIC_CLASS_NAME_UPPER}_SRV_LOG_FILE")
    log_file_prefix = log_file_prefix or _get_env_var(f"{MAGIC_CLASS_NAME_UPPER}_LOG_FILE_PREFIX")
    log_file_mode = log_file_mode or _get_env_var(f"{MAGIC_CLASS_NAME_UPPER}_LOG_FILE_MODE")

    # create file log handler
    create_file_logger_error_message = None
    try:
        if log_level or log_file or log_file_mode or log_file_prefix:

            log_level = log_level or logging.DEBUG
            log_file = log_file or f"{log_file_prefix or 'kqlmagic'}-srv-{kernel_id}.log"
            # handler's default mode is 'a' (append)
            log_file_mode = (log_file_mode or "w").lower()[:1]
            file_handler = logging.FileHandler(log_file, mode=log_file_mode)
            file_handler.setLevel(log_level)
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

            if log_file_mode == "a":
                logger.info("\n\n----------------------------------------------------------------------\n\n")

            now = datetime.now()
            logger.info(f"start date {now.isoformat()}")
            logger.info(f"logger level {log_level}\n")

    except Exception as e:
        create_file_logger_error_message = f"failed to create file log handler. log_level: {log_level}, log_file: {log_file}, " \
                                           f"log_file_prefix: {log_file_prefix}, log_file_mode: {log_file_mode}, error: {e}"

    # create console log handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level or logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    if create_file_logger_error_message is not None:
        logger.error(f"{create_file_logger_error_message}, continue log to console only.")
    return logger


class ParentHeartbeatMonitor(Thread):
    """ A heartbeat daemon thread that terminates the program immediately
    when the parent process no longer exists.
    """

    def __init__(self)->None:
        super(ParentHeartbeatMonitor, self).__init__()
        self.daemon = True
        self._stop_event = Event()
        self._heartbeat_event = Event()


    def run(self)->None:
        # We cannot use os.waitpid because it works only for child processes.

        count = 60
        logger.info(f"ParentHeartbeatMonitor for parent, parent is running.")

        while True:
            try:
                if count == 0:
                    count = 60
                    if self.heartbeated():
                        self._heartbeat_event.clear()
                        logger.info(f"ParentHeartbeatMonitor for parent, parent is running.")
                    else:
                        logger.info(f"ParentHeartbeatMonitor stopped to receive heartbeat from parent, shutting down.")
                        os_exit(0)
                        break
                count -= 1

                time.sleep(1.0)

                if self.stopped():
                    logger.info(f"ParentHeartbeatMonitor was asked to stop, exit.")
                    break

            except Exception as ex:
                error_message = f"ParentHeartbeatMonitor failed. error: {ex}"
                logger.error(f"{error_message}, continue monitoring.")


    def stop(self)->None:
        self._stop_event.set()


    def stopped(self)->bool:
        return self._stop_event.is_set()


    def heartbeat(self)->None:
        self._heartbeat_event.set()


    def heartbeated(self)->bool:
        return self._heartbeat_event.is_set()


class ParentProcessIdValueMonitor(Thread):
    """ A Unix-specific daemon thread that terminates the program immediately
    when the parent process no longer exists.
    """

    def __init__(self, platform:str)->None:
        super(ParentProcessIdValueMonitor, self).__init__()
        if platform == 'win32':
            raise Exception("specified liveness_mode: 'ppid' is not supported by 'win32' platform")
        elif parent_id == 1:
            # PID 1 (init) is special and will never go away,
            # only be reassigned.
            # Parent polling doesn't work if ppid == 1 to start with.
            raise Exception("specified liveness_mode: 'ppid' cannot work with parent_id == 1")
        self.daemon = True


    def run(self)->None:
        # We cannot use os.waitpid because it works only for child processes.

        count = 1

        while True:
            try:
                if count == 0:
                    count = 60
                    logger.info(f"ParentProcessIdValueMonitor for ppid {os.getppid()}, parent is running.")
                count -= 1

                time.sleep(1.0)

                if os.getppid() == 1:
                    logger.info(f"Parent appears to have exited, shutting down.")
                    os_exit(0)
                    break

            except OSError as e:
                if e.errno != errno.EINTR:
                    error_message = f"ParentProcessIdValueMonitor failed. error: {e}"
                    logger.error(f"{error_message}, continue monitoring.")
                    # raise

            except Exception as ex:
                error_message = f"ParentProcessIdValueMonitor failed. error: {ex}"
                logger.error(f"{error_message}, continue monitoring.")


test_disabled_modules = [module.strip() for module in (_get_env_var(f"{MAGIC_CLASS_NAME_UPPER}_TEST_DISABLE_MODULES") or "").split(",")]


def _get_import_module(module_name:str, message:str=None, package_name:str=None, dont_throw:bool=False):
    try:
        import importlib
        module = importlib.import_module(module_name)
        if module_name in test_disabled_modules:
            message = f", {message}" if message else ""
            message = f"module was disabled by {MAGIC_CLASS_NAME_UPPER}_TEST_DISABLE_MODULES{message}"
            raise Exception(message)
        return module
    except:
        if dont_throw:
            pass
        else:
            message = f", {message}" if message else ""
            import_error_message = f"failed to import '{module_name}' from '{package_name or module_name}'{message}"
            raise NotImplementedError(import_error_message)


class ParentProcessStateMonitor(Thread):
    """ A Windows-specific daemon thread that terminates the program immediately
    when the parent process no longer exists.
    """


    def __init__(self, parent_id:int=None)->None:
        super(ParentProcessStateMonitor, self).__init__()

        self.daemon = True
        self.parent_id = parent_id
        self.parent_create_time = None
        self.psutil = _get_import_module("psutil", message="won't be able to auto exit after parent's process exit")


    def run(self)->None:
        """ Run the monitor loop. This method never returns.
        """

        count = 0
        # Listen forever.
        while True:
            try:
                if count == 0:
                    count = 60
                    logger.info(f"ParentProcessStateMonitor for ppid {self.parent_id}, parent is running.")
                count -= 1

                time.sleep(1.0)

                parent_proc_not_found = True
                for proc in self.psutil.process_iter():
                    if proc.pid == self.parent_id:
                        proc_create_time = proc.create_time()
                        self.parent_create_time = self.parent_create_time or proc_create_time
                        # check proc.create_time to make sure the parent_id is not resued
                        if self.parent_create_time == proc_create_time:
                            parent_proc_not_found = False
                            break

                if parent_proc_not_found:
                    logger.info("Parent appears to have exited, shutting down.")
                    os_exit(0)
                    break
                    
            except OSError as e:
                if e.errno != errno.EINTR:
                    error_message = f"ParentProcessStateMonitor failed. error: {e}"
                    logger.error(f"{error_message}, continue monitoring.")
                    # raise

            except Exception as ex:
                error_message = f"ParentProcessStateMonitor failed. error: {ex}"
                logger.error(f"{error_message}, continue monitoring.")



DEFAULT_PORT = "5000"
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PROTOCOL = "http"


parent_monitor = None
parent_kernel_id = None
base_folder = None
os_exit_started = None
params = {}
folder_list = []


app = Flask("kqlmagic_temp_files_server")


@app.after_request
def after_request_func(response:Response)->Response:
    """disable server and client cache"""
    try:
        if request is not None:
            logger.debug(f"{request.method} {request.url} - {response.status_code}")

        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate, public, max-age=0"
        response.headers["Expires"] = 0
        response.headers["Pragma"] = "no-cache"


    except Exception as ex:
        error_message = f"after_request_func failed. error: {ex}"
        logger.error(f"{error_message}.")

    return response


@app.route('/files/<foldername>/<kernel_id>/<filename>')
def files_1(foldername:str, kernel_id:str, filename:str)->Response:
    """return content of filename as the response body."""
    return _files(foldername, kernel_id, filename)


@app.route('/files/<foldername0>/<foldername1>/<kernel_id>/<filename>')
def files_2(foldername0:str, foldername1:str, kernel_id:str, filename:str)->Response:
    """return content of filename as the response body."""
    foldername = f"{foldername0}/{foldername1}"
    return _files(foldername, kernel_id, filename)


@app.route('/files/<foldername0>/<foldername1>/<foldername2>/<kernel_id>/<filename>')
def files_3(foldername0:str, foldername1:str, foldername2:str, kernel_id:str, filename:str)->Response:
    """return content of filename as the response body."""
    foldername = f"{foldername0}/{foldername1}/{foldername2}"
    return _files(foldername, kernel_id, filename)


def _files(foldername:str, kernel_id:str, filename:str)->Response:
    """return content of filename as the response body."""
    try:
        _kernel_id = request.args.get("kernelid")

        if len(folder_list) == 0 or f"{foldername}/{kernel_id}" in folder_list:
            err_resp = check_path(foldername, kernel_id, filename)
            if err_resp is not None:
                return err_resp
            file_path = f"{base_folder}/{foldername}/{kernel_id}/{filename}"

            logger.debug(f"/files/{foldername}/{kernel_id}/{filename} - {file_path}")
            return send_file(file_path)

        else:
            error_message = f"folder {foldername} not in {folder_list}"
            logger.error(f"{error_message}.")
            return make_response(f"{error_message}, internal error", 404)

    except Exception as ex:
        error_message = f"/files/<foldername>/<kernel_id>/<filename> failed, error: {ex}"
        logger.error(f"{error_message}.")
        return make_response(f"{error_message}, internal error", 500)


@app.route('/folders/<foldername>')
def folders(foldername:str)->Response:
    """return content of filename as the response body."""
    try:
        _kernel_id = request.args.get("kernelid")
        _filename = request.args.get("filename")

        if len(folder_list) == 0 or f"{foldername}/{_kernel_id}" in folder_list:
            err_resp = check_path(foldername, _kernel_id, _filename)
            if err_resp is not None:
                return err_resp
            file_path = f"{base_folder}/{foldername}/{_kernel_id}/{_filename}"

            logger.debug(f"folders/{foldername} - {file_path}")
            return send_file(file_path)

        else:
            error_message = f"/folder {foldername} not in {folder_list}"
            logger.error(f"{error_message}.")
            return make_response(f"{error_message}, internal error", 404)

    except Exception as ex:
        error_message = f"/folders/<foldername> failed. error: {ex}."
        logger.error(f"{error_message}.")
        return make_response(f"{error_message}, internal error", 500)
    


@app.route('/ping')
def ping()->Union[str,Response]:
    """print 'pong' as the response body."""
    try:
        _kernel_id = request.args.get("kernelid")

        pong = f"pong kernelid: {_kernel_id}"
        logger.debug(f"/ping - {pong}")
        return pong

    except Exception as ex:
        error_message = f"/ping failed. error: {ex}"
        logger.error(f"{error_message}.")
        return make_response(f"{error_message}, internal error", 500)


@app.route('/heartbeat')
def heartbeat()->Union[str,Response]:
    """print 'heartbeat' as the response body."""
    try:
        _kernel_id = request.args.get("kernelid")

        if parent_monitor.__class__.__name__ == ParentHeartbeatMonitor:
            parent_monitor.heartbeat()

        heartbeat = f"heartbeat kernelid: {_kernel_id}"
        logger.debug(f"/heartbeat - {heartbeat}")
        return heartbeat

    except Exception as ex:
        error_message = f"/heartbeat failed. error: {ex}"
        logger.error(f"{error_message}.")
        return make_response(f"{error_message}, internal error", 500)


@app.route('/webbrowser')
def webbrowser()->str:
    """open web browser."""
    try:
        _encoded_url = request.args.get("url")
        _kernel_id = request.args.get("kernelid")

        import urllib.parse
        import webbrowser

        url = urllib.parse.unquote(_encoded_url)
        webbrowser.open(url, new=1, autoraise=True)
        logger.debug(f"/webbrowser - {url} - close after 1 sec")

    except Exception as ex:
        error_message = f"/webbrowser failed. error: {ex}"
        logger.error(f"{error_message}.")
        pass

    return """<!DOCTYPE html>
            <html><body>
            <script>
            window.close();
            //setTimeout(function(){ window.close(); }, 1*1000);
            </script>
            </body></html>"""


@app.route('/abort')
def abort()->str:
    """aborts the process"""
    try:
        _kernel_id = request.args.get("kernelid")
        logger.debug(f"/abort")

        os_exit(0)

    except Exception as ex:
        error_message = f"/abort failed. error: {ex}"
        logger.error(f"{error_message}.")
        raise ex

    return ''


def check_path(foldername:str, kernel_id:str, filename:str)->Response:
    err_resp = None
    try:
        error_message = None
        if not os.path.exists(f"{base_folder}"):
            error_message = f"Base folder '{base_folder}' not found"

        elif not os.path.exists(f"{base_folder}/{foldername}"):
            error_message = f"Folder {base_folder}/{foldername} not found"

        elif not os.path.exists(f"{base_folder}/{foldername}/{kernel_id}"):
            error_message = f"Folder {base_folder}/{foldername}/{kernel_id}  not found"

        elif not os.path.exists(f"{base_folder}/{foldername}/{kernel_id}/{filename}"):
            error_message = f"File {base_folder}/{foldername}/{kernel_id}/{filename} not found"

        if error_message is not None:
            err_resp = make_response(error_message, 404)
            logger.error(f"check_path failed, {error_message}. code: 404.")

    except Exception as ex:
        error_message = f"File {base_folder}/{foldername}/{kernel_id}/{filename}, error: {ex}"
        logger.error(f"check_path failed, {error_message}, code: 500")
        err_resp = make_response(f"{error_message}, internal error", 500)

    return err_resp


def init_parent_monitor(platform:str, parent_id_str:str, liveness_mode:str)->Union[ParentHeartbeatMonitor,ParentProcessStateMonitor,ParentProcessIdValueMonitor]:
    if parent_id_str is not None:
        try:
            parent_id = int(parent_id_str) 

            if liveness_mode == "heartbeat":
                return ParentHeartbeatMonitor()

            elif liveness_mode == "parent_process_id_value":
                return ParentProcessIdValueMonitor(platform)

            elif liveness_mode == "parent_process_state":
                return ParentProcessStateMonitor(parent_id)

            elif liveness_mode == "show":
                return

            elif liveness_mode == None:
                #
                # legacy
                #
                if platform == 'win32':
                    return ParentProcessStateMonitor(parent_id)
                else:
                    try:
                        return ParentProcessIdValueMonitor(platform)
                    except:
                        return ParentProcessStateMonitor(parent_id)
            else:
                raise Exception(f"unknown liveness mode: {liveness_mode}")


       
        except Exception as ex:
            error_message = f"init_parent_monitor got an error: {ex}, parent monitor (liveness_mode: {liveness_mode} disabled"
            logger.error(f"{error_message}.")


def os_exit(code:int)->None:
    global os_exit_started
    if os_exit_started is None:
        os_exit_started = True
        try:
            logger.info(f"OS_EXIT code: {code}!!!")
            to_clean = params.get("clean", None)
            if to_clean is not None and len(folder_list) > 0:
                for folder in folder_list:
                    folder_path = None
                    try:
                        folder_path = f"{base_folder}/{folder}"
                        logger.info(f"start to clean folder: {folder_path}")
                        for filename in os.listdir(folder_path):
                            file_path = None
                            try:
                                file_path = f"{base_folder}/{folder}/{filename}"
                                os.unlink(file_path)
                            except Exception as e:
                                logger.error(f"failed to clean file: {file_path}, reason: {e}")
                        os.rmdir(folder_path)
                        logger.info(f"clean folder: {folder_path} done")
                    except Exception as e:
                        logger.error(f"failed to clean folder: {folder_path}, reason: {e}")
        except Exception as e:
            logger.error(f"failed to fully clean, reason: {e}")
        logger.info(f"clean finished")
        # time.sleep(60.0)
        os._exit(code)


if __name__ == "__main__":
    try:
        key = None
        for arg in sys.argv[1:]:
            kv = arg.split("=")
            if arg.startswith('-') and len(kv) == 2:
                key = kv[0][1:]
                value = kv[1]
                params[key] = value
                key= None

            elif arg.startswith('-'):
                key = arg[1:]
                params[key] = True
            elif key is not None:
                params[key] = arg
                key = None

        parent_kernel_id = params.get("parent_kernel_id")  
        logger = init_logger(parent_kernel_id)
        logger.debug(f"__name__ argv: {sys.argv[1:]}")
        logger.debug(f"__name__ params: {params}")
        base_folder = params.get("base_folder")       
        if base_folder is not None:
            base_folder = base_folder[:-1] if base_folder.endswith("/") else base_folder
            folders_str = params.get("folders", "")
            folder_list = [folder[:-1] if folder.endswith('/') else folder for folder in folders_str.split(",")]
            port = params.get("port", DEFAULT_PORT)
            host = params.get("host", DEFAULT_HOST)
            parent_id = params.get("parent_id", None) 
            liveness_mode = params.get("liveness_mode")
            parent_monitor = init_parent_monitor(sys.platform, parent_id, liveness_mode)
            if parent_monitor is not None:
                logger.info(f"start parent_monitor for parent id: {parent_id}")
                parent_monitor.start()
            logger.info(f" * parent id: {parent_id}")
            logger.info(f" * Base folder: {base_folder}")
            logger.info(f" * Folder list: {folder_list}")
            logger.info(f" * ")
            
            logger.info(f" **** ")
            app.run(host=host, port=port, debug=False, use_reloader=False, use_debugger=False)

        else:
            error_message = f"__name__ failed. base_folder value is None"
            logger.error(f"{error_message}.")
            time.sleep(5 * 60 * 1.0)
            os._exit(1)

    except Exception as ex:
        error_message = f"__name__ failed. error: {ex}"
        # this print is not for debug
        print(f"{error_message}.")
        try:
            if logger is not None:
                logger.error(f"{error_message}.")
        except:
            pass

        time.sleep(5 * 60 * 1.0)
        os._exit(1)

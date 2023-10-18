# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

import os
import logging
import datetime
import uuid
import traceback
from typing import Any, List, Dict


from .constants import Constants
from .my_utils import get_env_var
from .ipython_api import IPythonAPI


isNullLogger:bool = False


def initialize(log_level:str=None, log_file:str=None, log_file_prefix:str=None, log_file_mode:str=None)->None:
    global isNullLogger
    log_level = log_level or get_env_var(f"{Constants.MAGIC_CLASS_NAME_UPPER}_LOG_LEVEL")
    log_file = log_file or get_env_var(f"{Constants.MAGIC_CLASS_NAME_UPPER}_LOG_FILE")
    log_file_prefix = log_file_prefix or get_env_var(f"{Constants.MAGIC_CLASS_NAME_UPPER}_LOG_FILE_PREFIX")
    log_file_mode = log_file_mode or get_env_var(f"{Constants.MAGIC_CLASS_NAME_UPPER}_LOG_FILE_MODE")
    if log_level or log_file or log_file_mode or log_file_prefix:
        kernel_id = IPythonAPI.get_notebook_kernel_id() or "kernel_id"

        log_level = log_level or logging.DEBUG
        log_file = log_file or f"{log_file_prefix or 'kqlmagic'}-{kernel_id}.log"
        # handler's default mode is 'a' (append)
        log_file_mode = (log_file_mode or "w").lower()[:1]
        log_handler = logging.FileHandler(log_file, mode=log_file_mode)
        isNullLogger = False
    else:
        log_handler = logging.NullHandler()
        isNullLogger = True

    set_logging_options({'level': log_level, 'handler': log_handler})
    set_logger(Logger())

    if log_file:
        if log_file_mode == "a":
            logger().debug("\n\n----------------------------------------------------------------------")
        now = datetime.datetime.now()

        logger().debug(f"start date {now.isoformat()}\n")
        logger().debug(f"logger level {log_level}\n")
        logger().debug("logger init done")


def create_log_context(correlation_id:str=None)->Dict[str,str]:
    return {"correlation_id": correlation_id or str(uuid.uuid4())}


def set_logging_options(options:Dict[str,Any]=None)->None:
    """Configure logger, including level and handler spec'd by python
    logging module.

    Basic Usages::
        >>>set_logging_options({
        >>>  'level': 'DEBUG'
        >>>  'handler': logging.FileHandler(<file-name>) # file name can be 
        >>>})
    """
    if options is None:
        options = {}
    logger = logging.getLogger(Constants.LOGGER_NAME)

    logger.setLevel(options.get("level", logging.ERROR) or logging.ERROR)

    handler = options.get("handler")
    if handler:
        handler.setLevel(logger.level)
        logger.addHandler(handler)


def get_logging_options()->Dict[str,Any]:
    """Get logging options

    :returns: a dict, with a key of 'level' for logging level.
    """
    logger = logging.getLogger(Constants.LOGGER_NAME)
    level = logger.getEffectiveLevel()
    return {"level": logging.getLevelName(level)}


#    log_context = log.create_log_context(correlation_id)
#    logger = log.Logger('SomeComponent', log_context)
class Logger(object):
    """wrapper around python built-in logging to log correlation_id, and stack
    trace through keyword argument of 'log_stack_trace'
    """

    def __init__(self, component_name:str=None, log_context:Dict[str,str]=None)->None:
        # if not log_context:
        #     raise AttributeError('Logger: log_context is a required parameter')

        self._component_name = component_name
        self.log_context = log_context
        self._logging = logging.getLogger(Constants.LOGGER_NAME)
        self._current_log_buffer = []


    def _log_message(self, msg:str, log_stack_trace:bool=None)->str:
        formatted = ""

        if self.log_context:
            correlation_id = self.log_context.get("correlation_id")
            if correlation_id:
                formatted = f"{correlation_id} - "

        if self._component_name:
            formatted += f"{self._component_name}:"

        formatted += msg

        if log_stack_trace:
            formatted += f"\nStack:\n{traceback.format_stack()}"

        self._current_log_buffer.append(formatted)
        return formatted
        

    def isNull(self)->bool:
        return isNullLogger


    def getCurrentLogMessages(self)->List[str]:
        return self._current_log_buffer


    def resetCurrentLogMessages(self)->None:
        self._current_log_buffer = []


    def critical(self, msg:str, *args, **kwargs)->None:
            try:
                log_stack_trace = kwargs.pop("log_stack_trace", None)
                msg = self._log_message(msg, log_stack_trace)
                self._logging.critical(msg, *args, **kwargs)
            except Exception as e:
                self._logException(e, "critical")


    def error(self, msg:str, *args, **kwargs)->None:
        try:
            log_stack_trace = kwargs.pop("log_stack_trace", None)
            msg = self._log_message(msg, log_stack_trace)
            self._logging.error(msg, *args, **kwargs)
        except Exception as e:
            self._logException(e, "error")


    def warn(self, msg:str, *args, **kwargs)->None:
        try:
            log_stack_trace = kwargs.pop("log_stack_trace", None)
            msg = self._log_message(msg, log_stack_trace)
            self._logging.warning(msg, *args, **kwargs)
        except Exception as e:
            self._logException(e, "warn")


    def info(self, msg:str, *args, **kwargs)->None:
        try:
            log_stack_trace = kwargs.pop("log_stack_trace", None)
            msg = self._log_message(msg, log_stack_trace)
            self._logging.info(msg, *args, **kwargs)
        except Exception as e:
            self._logException(e, "info")


    def debug(self, msg:str, *args, **kwargs)->None:
        try:
            log_stack_trace = kwargs.pop("log_stack_trace", None)
            msg = self._log_message(msg, log_stack_trace)
            self._logging.debug(msg, *args, **kwargs)
        except Exception as e:
            self._logException(e, "debug")


    def exception(self, msg:str, *args, **kwargs)->None:
        try:
            log_stack_trace = kwargs.pop("log_stack_trace", None)
            msg = self._log_message(msg, log_stack_trace)
            self._logging.exception(msg, *args, **kwargs)
        except Exception as e:
            self._logException(e, "exception")


    def _logException(self, ex:Exception, level:str, **kwargs)->None:
        try:
            log_stack_trace = kwargs.pop("log_stack_trace", None)
            msg = f"failed to log level {level} record"
            try:
                msg = self._log_message(msg, log_stack_trace)
            except:
                pass
            self._logging.exception(msg, exc_info=ex)
        except:
            pass


current_logger:Logger = None


def logger()->Logger:
    return current_logger


def set_logger(new_logger:Logger)->Logger:
    global current_logger
    current_logger = new_logger
    return current_logger


initialize()

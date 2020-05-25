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


from .constants import Constants
from .ipython_api import IPythonAPI


def _get_env_var(var_name:str)->str:
    value = os.getenv(var_name)
    if value:
        # value = value.strip().upper().replace("_", "").replace("-", "")
        if value.startswith("'") or value.startswith('"'):
            value = value[1:-1].strip()
    return value


def initialize(log_level=None, log_file=None, log_file_prefix=None, log_file_mode=None):
    log_level = log_level or _get_env_var(f"{Constants.MAGIC_CLASS_NAME_UPPER}_LOG_LEVEL")
    log_file = log_file or _get_env_var(f"{Constants.MAGIC_CLASS_NAME_UPPER}_LOG_FILE")
    log_file_prefix = log_file_prefix or _get_env_var(f"{Constants.MAGIC_CLASS_NAME_UPPER}_LOG_FILE_PREFIX")
    log_file_mode = log_file_mode or _get_env_var(f"{Constants.MAGIC_CLASS_NAME_UPPER}_LOG_FILE_MODE")
    if log_level or log_file or log_file_mode or log_file_prefix:
        kernel_id = IPythonAPI.get_notebook_kernel_id() or "kernel_id"


        log_level = log_level or logging.DEBUG
        log_file = log_file or f"{log_file_prefix or 'kqlmagic'}-{kernel_id}.log"
        # handler's default mode is 'a' (append)
        log_file_mode = (log_file_mode or "w").lower()[:1]
        log_handler = logging.FileHandler(log_file, mode=log_file_mode)
    else:
        log_handler = logging.NullHandler()

    set_logging_options({ 'level': log_level, 'handler': log_handler})
    set_logger(Logger())

    if log_file:
        if log_file_mode == "a":
            logger().debug("\n\n----------------------------------------------------------------------")
        now = datetime.datetime.now()

        logger().debug(f"start date {now.isoformat()}\n")
        logger().debug(f"logger level {log_level}\n")
        logger().debug("logger init done")


def create_log_context(correlation_id=None):
    return {"correlation_id": correlation_id or str(uuid.uuid4())}


def set_logging_options(options=None):
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


def get_logging_options():
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

    def __init__(self, component_name=None, log_context=None):
        # if not log_context:
        #     raise AttributeError('Logger: log_context is a required parameter')

        self._component_name = component_name
        self.log_context = log_context
        self._logging = logging.getLogger(Constants.LOGGER_NAME)


    def _log_message(self, msg, log_stack_trace=None):
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

        return formatted


    def critical(self, msg, *args, **kwargs):
        log_stack_trace = kwargs.pop("log_stack_trace", None)
        msg = self._log_message(msg, log_stack_trace)
        self._logging.critical(msg, *args, **kwargs)


    def error(self, msg, *args, **kwargs):
        log_stack_trace = kwargs.pop("log_stack_trace", None)
        msg = self._log_message(msg, log_stack_trace)
        self._logging.error(msg, *args, **kwargs)


    def warn(self, msg, *args, **kwargs):
        log_stack_trace = kwargs.pop("log_stack_trace", None)
        msg = self._log_message(msg, log_stack_trace)
        self._logging.warning(msg, *args, **kwargs)


    def info(self, msg, *args, **kwargs):
        log_stack_trace = kwargs.pop("log_stack_trace", None)
        msg = self._log_message(msg, log_stack_trace)
        self._logging.info(msg, *args, **kwargs)


    def debug(self, msg, *args, **kwargs):
        log_stack_trace = kwargs.pop("log_stack_trace", None)
        msg = self._log_message(msg, log_stack_trace)
        self._logging.debug(msg, *args, **kwargs)


    def exception(self, msg, *args, **kwargs):
        log_stack_trace = kwargs.pop("log_stack_trace", None)
        msg = self._log_message(msg, log_stack_trace)
        self._logging.exception(msg, *args, **kwargs)


current_logger = None

def logger():
    return current_logger


def set_logger(new_logger):
    global current_logger
    current_logger = new_logger
    return current_logger


initialize()
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
from ipykernel import (get_connection_info)
from Kqlmagic.constants import Constants

def _get_kql_magic_log_level():
    log_level = os.getenv("{0}_LOG_LEVEL".format(Constants.MAGIC_CLASS_NAME.upper()))
    if log_level:
        log_level = log_level.strip().upper().replace("_", "").replace("-", "")
        if log_level.startswith("'") or log_level.startswith('"'):
            log_level = log_level[1:-1].strip()
    return log_level

def initialize():
    log_level = _get_kql_magic_log_level()
    log_file = os.getenv("{0}_LOG_FILE".format(Constants.MAGIC_CLASS_NAME.upper()))
    log_file_prefix = os.getenv("{0}_LOG_FILE_PREFIX".format(Constants.MAGIC_CLASS_NAME.upper()))
    log_file_mode = os.getenv("{0}_LOG_FILE_MODE".format(Constants.MAGIC_CLASS_NAME.upper()))
    if log_level or log_file or log_file_mode or log_file_prefix:
        connection_info = get_connection_info(unpack=True)
        key = connection_info.get("key").decode(encoding="utf-8")
        log_level = log_level or logging.DEBUG
        log_file = log_file or ((log_file_prefix or 'Kqlmagic') + '-' + key + '.log')
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

        logger().debug("start date %s\n", now.isoformat())
        logger().debug("logger level %s\n", log_level)
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
                formatted = "{} - ".format(correlation_id)

        if self._component_name:
            formatted += "{}:".format(self._component_name)

        formatted += msg

        if log_stack_trace:
            formatted += "\nStack:\n{}".format(traceback.format_stack())

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


def logger():
    global current_logger
    return current_logger


def set_logger(new_logger):
    global current_logger
    current_logger = new_logger
    return current_logger

initialize()
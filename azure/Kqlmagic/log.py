#-------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
#--------------------------------------------------------------------------

import logging
import uuid
import traceback
from Kqlmagic.constants import Constants

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

    logger.setLevel(options.get("level", logging.ERROR))

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


def logger():
    global current_logger
    return current_logger


def set_logger(new_logger):
    global current_logger
    current_logger = new_logger
    return current_logger

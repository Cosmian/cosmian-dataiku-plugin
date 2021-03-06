import logging


class LogLevelContext:
    """ ContextManager to override the log level for the given loggers in the context
        Example:
            my_logger = logging.getLogger("my_logger_name")
            my_logger.setLevel(logging.DEBUG)

            log_level_context = LogLevelContext(logging.CRITICAL, ["my_logger_name"])

            with log_level_context:
                my_logger.debug("Should NOT be emitted")

            my_logger.debug("Should be emitted")

            => Only the last log will be emitted

        Args:
            log_level (int): the level of log to which the loggers will be set. Can be logging.CRITICAL, logging.WARN...
            loggers_names (list of str): list of loggers names to which the log level should be overridden
            disable (bool): to disable all impact of the context manager. If True, the ContextManager does nothing.
            show_first_occurrence (bool): whether to override the log level when the context is used for the first time

    """
    def __init__(self, log_level, loggers_names, disable=False, show_first_occurrence=False):
        self.log_level = log_level
        self.disable = disable

        self._loggers_levels = []
        self._loggers = []
        for logger_name in loggers_names:
            logger = logging.getLogger(logger_name)
            self._loggers_levels.append(logger.level)
            self._loggers.append(logger)

        self._show_first_occurrence = show_first_occurrence
        self._is_first = True

    def _should_override_log_level(self):
        if self.disable:
            return False
        if self._show_first_occurrence and self._is_first:
            return False

        return True

    def __enter__(self):
        if self._should_override_log_level():
            for logger in self._loggers:
                logger.setLevel(self.log_level)
        self._is_first = False

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._should_override_log_level():
            for index, logger in enumerate(self._loggers):
                logger.setLevel(self._loggers_levels[index])

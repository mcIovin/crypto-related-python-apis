import sys
import logging
from logging.handlers import RotatingFileHandler


def setup_logging(destination='console', # noqa: C901
                  file_logging_level='debug',
                  console_logging_level='info',
                  full_path_to_log_file=''):
    """Setup logging.
    The 'destination' parameter can take one of two options:
      - 'file'
      - 'console'  (default)
      - both
      """
    list_of_acceptable_destinations = ['file', 'console', 'both']
    list_of_handlers = []
    list_of_warnings = []

    initial_destination = destination
    if destination not in list_of_acceptable_destinations:
        # if an unacceptable string was passed as the wanted destination, then we will issue a warning
        # once the logging is setup, and we'll set the default (console)
        destination = 'console'
        list_of_warnings.append("An invalid logging destination was passed: " + initial_destination + ". The default "
                                "destination (console) was set instead.")

    if (destination == 'file') or (destination == 'both'):
        if full_path_to_log_file:
            # set the logging level for the logs that will be written to a file
            file_level = logging.DEBUG
            if file_logging_level == 'info':
                file_level = logging.INFO
            elif file_logging_level == 'warning':
                file_level = logging.WARNING
            elif file_logging_level == 'error':
                file_level = logging.ERROR
            # configure the rotating file logging handler
            handler_rotating_file = RotatingFileHandler(filename=full_path_to_log_file, mode='a',
                                                        maxBytes=5 * 1024 * 1024, backupCount=20)
            handler_rotating_file.setLevel(file_level)
            list_of_handlers.append(handler_rotating_file)
        else:
            destination = 'console'
            list_of_warnings.append(
                "If destination is 'file' or 'both', a path for a log file MUST be provided. The default "
                "destination (console) was set instead.")

    if (destination == 'console') or (destination == 'both'):
        # set the logging level for the logs that will be sent to stdout
        console_level = logging.INFO
        if console_logging_level == 'debug':
            console_level = logging.DEBUG
        elif console_logging_level == 'warning':
            console_level = logging.WARNING
        elif console_logging_level == 'error':
            console_level = logging.ERROR
        # configure the stdout logging handler
        handler_stdout = logging.StreamHandler(sys.stdout)
        handler_stdout.setLevel(console_level)
        list_of_handlers.append(handler_stdout)

    logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s',
                        datefmt='%Y%m%d-%H:%M:%S',
                        level=logging.DEBUG,
                        handlers=list_of_handlers)

    for a_warning in list_of_warnings:
        logging.warning(a_warning)
# ------------------------ END FUNCTION ------------------------ #

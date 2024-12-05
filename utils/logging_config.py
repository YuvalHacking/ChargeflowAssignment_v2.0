from contextlib import contextmanager
import logging
import colorlog

class IndentLoggerAdapter(logging.LoggerAdapter):
    # Override the process method to add indentation to the log message
    def process(self, msg, kwargs):
        return '    ' * self.extra['indent_level'] + msg, kwargs

# Configure colored logging
stream_handler = colorlog.StreamHandler()
stream_handler.setFormatter(colorlog.ColoredFormatter(
    '%(log_color)s%(asctime)s - %(levelname)s - %(message)s',
    log_colors={
        'DEBUG': 'cyan',
        'INFO': 'green',
        'WARNING': 'yellow',
        'ERROR': 'red',
        'CRITICAL': 'bold_red',
    }
))
logger = colorlog.getLogger()
logger.addHandler(stream_handler)
logger.setLevel(logging.INFO)

# Logging with custom indentation
@contextmanager
def log_indent(indent_level=1):
    
    # Create an instance of IndentLoggerAdapter with the specified indent level
    adapter = IndentLoggerAdapter(logger, {'indent_level': indent_level})
    original_factory = logging.getLogRecordFactory()
    
    # Define a new log record factory that adds indentation to the log message
    def record_factory(*args, **kwargs):
        record = original_factory(*args, **kwargs)
        record.msg = '    ' * indent_level + record.msg
        return record
    
     # Set the new log record factory
    logging.setLogRecordFactory(record_factory)
    try:
        yield adapter
    finally:
         # Restore the original log record factory after the context
        logging.setLogRecordFactory(original_factory)

import luigi
import logging
import util

def setup_logging():
    util.ensuredir('log')
    log_path = 'log/sciluigi_run_%s.log' % util.timepath()

    # Formatter
    luigi_log_formatter = logging.Formatter('%(asctime)s %(levelname)s LUIGI    %(message)s','%Y-%m-%d %H:%M:%S')
    sciluigi_log_formatter = logging.Formatter('%(asctime)s %(levelname)s SCILUIGI %(message)s','%Y-%m-%d %H:%M:%S')

    # Stream handler (for STDERR)
    luigi_stream_handler = logging.StreamHandler()
    luigi_stream_handler.setFormatter(luigi_log_formatter)
    luigi_stream_handler.setLevel(logging.INFO)

    sciluigi_stream_handler = logging.StreamHandler()
    sciluigi_stream_handler.setFormatter(sciluigi_log_formatter)
    sciluigi_stream_handler.setLevel(logging.INFO)

    # File handler
    luigi_file_handler = logging.FileHandler(log_path)
    luigi_file_handler.setFormatter(luigi_log_formatter)
    luigi_file_handler.setLevel(logging.DEBUG)

    sciluigi_file_handler = logging.FileHandler(log_path)
    sciluigi_file_handler.setFormatter(sciluigi_log_formatter)
    sciluigi_file_handler.setLevel(logging.DEBUG)

    # Loggers
    luigi_logger = logging.getLogger('luigi-interface')
    luigi_logger.addHandler(luigi_stream_handler)
    luigi_logger.addHandler(luigi_file_handler)
    luigi_logger.setLevel(logging.DEBUG)
    luigi.interface.setup_interface_logging.has_run = True

    sciluigi_logger = logging.getLogger('sciluigi-interface')
    sciluigi_logger.addHandler(sciluigi_stream_handler)
    sciluigi_logger.addHandler(sciluigi_file_handler)
    sciluigi_logger.setLevel(logging.DEBUG)

setup_logging()

def run(*args, **kwargs):
    log = logging.getLogger('sciluigi-interface')
    luigi.run(*args, **kwargs)

def run_local():
    run(local_scheduler=True)

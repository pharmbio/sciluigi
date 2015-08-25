import luigi
import logging
import util

def run(*args, **kwargs):
    luigi.run(*args, **kwargs) 

def run_locally():
    luigi.run(local_scheduler=True)

def setup_logging():
    # Formatter
    log_formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s','%Y-%m-%d %H:%M:%S')

    # Handlers
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(log_formatter)
    stream_handler.setLevel(logging.INFO)

    util.ensuredir('log')
    file_handler = logging.FileHandler('log/sciluigi_run_%s.log' % util.timepath())
    file_handler.setFormatter(log_formatter)
    file_handler.setLevel(logging.DEBUG)

    # SciLuigi logger
    sciluigi_logger = logging.getLogger('sciluigi-interface')
    sciluigi_logger.addHandler(stream_handler)
    sciluigi_logger.addHandler(file_handler)
    sciluigi_logger.setLevel(logging.DEBUG)

    # Luigi logger
    luigi_logger = logging.getLogger('luigi-interface')
    luigi_logger.addHandler(stream_handler)
    luigi_logger.addHandler(file_handler)
    luigi_logger.setLevel(logging.DEBUG)

    # Mark luigi logging setup as done
    luigi.interface.setup_interface_logging.has_run = True

setup_logging()

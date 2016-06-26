import logging
import logging.handlers
import argparse
import json
import os
from datetime import datetime
from .dbmanager import start_database, record_submissions, fetch_submissions
from .handlers import build_handler
from .scrapers import build_scraper

LOGGER = logging.getLogger(__name__)


class Reactor:

    defaults = {
        'app_name': 'Show My Solutions',
        'engine_params': {},
        'submit_time_format': '%b %d %H:%M %Z',
        'path': '..',
        'logging': 'INFO',
        'sources': [],
        'handlers': [],
    }

    def __init__(self, config):
        self.options = dict(self.defaults)
        self.options.update(config)

        path = os.path.abspath(self.options['path'])
        level = config_logging(path, self.options['logging'])
        engine_params = self.options['engine_params']
        engine_params.setdefault('path', path)
        engine_params.setdefault('echo', level == logging.DEBUG)
        start_database(**engine_params)

        self.handlers = [build_handler(s, self) for s in self.options['handlers']]
        self.sources = [build_scraper(s, self) for s in self.options['sources']]

        LOGGER.debug('Loaded with configuration: %s', json.dumps(self.options, indent=2))

    def start(self):
        for s in self.sources:
            subs = s.fetch()
            LOGGER.debug('Fetched submissions: %s', subs)
            LOGGER.info('Fetched %s submission(s) from %s', len(subs), s.name)
            record_submissions(subs)
        LOGGER.info('All submissions are fetched')

        for h in self.handlers:
            subs = fetch_submissions(h.name)
            if subs:
                LOGGER.debug('Uploading submissions: %s', subs)
                h.upload(subs)
                LOGGER.info('Uploaded %s submission(s) to %s', len(subs), h.name)
        LOGGER.info('All submissions are uploaded')


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--config', metavar='path', required=False,
                        default=None, help='Path to the config file.')
    return parser.parse_args()


def config_logging(path, level):
    from . import __name__ as name

    def set_handler(hdlr):
        hdlr.setLevel(level)
        hdlr.setFormatter(fmt)
        logger.addHandler(hdlr)

    try:
        level = getattr(logging, level)
        assert isinstance(level, int)
    except (AttributeError, AssertionError):
        raise Value('Invalid logging level')
    logger = logging.getLogger(name)
    logger.setLevel(level)

    now = datetime.now()
    log_path = os.path.join(path, now.strftime('%b_%d'))
    os.makedirs(log_path, exist_ok=True)
    filename = os.path.join(log_path, '{}_{}.log'.format(now.strftime('%d-%m-%y-%H00'), name))
    fmt = logging.Formatter('%(asctime)s %(levelname)s - %(message)s', '%m-%d %H:%M')
    set_handler(logging.handlers.RotatingFileHandler(filename, maxBytes=10 * 2**20, backupCount=5))
    set_handler(logging.StreamHandler())

    return level


def get_config(config_path=None):
    if config_path is None:
        config_path = './example_configs/config.json'
        path = '.'
    else:
        path, _ = os.path.split(config_path)

    try:
        with open(config_path) as fin:
            config = json.load(fin)
    except FileNotFoundError:
        raise ValueError('Config file does not exist')

    config['path'] = path

    return config


def run():
    args = get_args()
    config = get_config(args.config)

    Reactor(config).start()


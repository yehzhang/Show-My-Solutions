import logging
import argparse
import json
import os.path
from datetime import datetime
import warnings
from .dbmanager import start_database, record_submissions, fetch_submissions, _reset_tables
from .handlers import build_handler
from .scrapers import build_scraper

LOGGER = logging.getLogger(__name__)
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))


class Reactor:

    defaults = {
        'app_name': 'Show My Solutions',
        'submit_time_format': '%b %d %H:%M %Z',
        'sources': [],
        'handlers': [],
    }

    def __init__(self, config):
        self.options = dict(self.defaults)
        self.options.update(config)
        self.options['root_dir'] = ROOT_DIR

        self.handlers = [build_handler(s, self) for s in self.options['handlers']]
        if not self.handlers:
            LOGGER.warning('No handler found')
        self.sources = [build_scraper(s, self) for s in self.options['sources']]
        if not self.sources:
            LOGGER.warning('No source found')

        LOGGER.debug('Loaded configuration: %s', json.dumps(self.options, indent=2))

    def start(self):
        LOGGER.info('Reactor started')
        try:
            num_subs = 0
            for s in self.sources:
                subs = s.fetch()
                LOGGER.debug('Fetched submissions: %s', subs)
                LOGGER.info("Fetched %s submission(s) from '%s'", len(subs), s.name)
                record_submissions(subs)
                num_subs += len(subs)
            LOGGER.info('Fetched %s submission(s) in total', num_subs)

            num_ups = 0
            for h in self.handlers:
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    subs = fetch_submissions(h.name)
                if subs:
                    LOGGER.debug('Handling submissions: %s', subs)
                    h.upload(subs)
                    LOGGER.info("Handled %s submission(s) by '%s'", len(subs), h.name)
                    num_ups += len(subs)
            LOGGER.info('Handled %s submission(s) in total', num_ups)

        except KeyboardInterrupt:
            pass

        finally:
            LOGGER.info('Reactor stopped')


def set_logging_level(level):
    from . import __name__ as name

    try:
        level = getattr(logging, level)
        assert isinstance(level, int)
    except (AttributeError, AssertionError):
        raise ValueError('Invalid logging level')
    logging.getLogger(name).setLevel(level)
    return level


def setup_logging():
    from . import __name__ as name

    logger = logging.getLogger(name)
    hdlr = logging.StreamHandler()
    fmt = logging.Formatter('%(asctime)s %(levelname)s - %(message)s', '%m-%d %H:%M')
    hdlr.setFormatter(fmt)
    logger.addHandler(hdlr)
    logger.setLevel(logging.INFO)


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', metavar='path', required=False, default=None,
                        help='Path to the config file.')
    parser.add_argument('-r', '--reset', required=False, action='store_true', default=False,
                        help='Reset the internal database')
    return parser.parse_args()


def get_config(config_path=None):
    config_path = './config.json' if not config_path else config_path
    try:
        with open(config_path) as fin:
            return json.load(fin)
    except FileNotFoundError:
        config_path = os.path.abspath(config_path)
        raise ValueError('Config file does not exist on the path: {}'.format(config_path))
    except IsADirectoryError:
        raise ValueError('Config file is a directory')
    except json.JSONDecodeError as e:
        raise ValueError('Invalid config file: {}'.format(e))


def run():
    args = get_args()
    setup_logging()

    try:
        if args.reset:
            if input('Reset the internal database? [y/n]: ').startswith('y'):
                start_database(path=ROOT_DIR)
                _reset_tables()
                LOGGER.info('Database has been reset')
            else:
                LOGGER.info('Reset aborted')

        else:
            config = get_config(args.config)

            level = set_logging_level(config.get('logging', 'INFO'))

            engine_params = config.get('engine_params', {})
            engine_params.setdefault('path', ROOT_DIR)
            engine_params.setdefault('echo', level <= logging.DEBUG)
            start_database(**engine_params)

            Reactor(config).start()
    except Exception as e:
        LOGGER.exception(e)
        return 1

    return 0

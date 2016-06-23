import logging
import requests
from .dbmanager import fetch_submissions, add_milestone
from .auth import TrelloAuth


LOGGER = logging.getLogger(__name__)


class _HandlerMeta(type):

    loaded = {}
    handlers = {}

    def __new__(cls, name, bases, nmspc):
        cls = super().__new__(cls, name, bases, nmspc)
        cls_name = nmspc['name']
        if cls_name:
            cls.handlers[cls_name] = cls
            LOGGER.info('Register Handler: {}'.format(cls_name))
        return cls

    @classmethod
    def get(cls, name, settings):
        if name not in cls.loaded:
            cls.loaded[name] = cls.handlers[name](settings)
        return cls.loaded[name]

build_handler = _HandlerMeta.get


class BaseHandler(metaclass=_HandlerMeta):

    name = None
    defaults = {}

    def __init__(self, settings):
        self.settings = settings
        self.config = dict(self.defaults)
        self.config.update(self.settings.get(self.name, {}))
        self.init_handler()
        LOGGER.debug("Handler '%s' has inited: %s", self.name, self.config)

    def init_handler(self):
        """Init configuration here."""
        raise NotImplementedError()

    def upload(self, submissions):
        raise NotImplementedError()


class TrelloHandler(BaseHandler):

    name = 'trello'
    defaults = {
        'app_key': '7a0445134100faef2f5bbbc4437a42e6',
        'submit_time_format': '%b %d %H:%M %Z',
        'user_token': None,
        'auth_expiration': '30days',
        # TODO Warn user about duplication, first come first chosen
        'target_board_name': None,
        'target_list_name': None,
    }

    def init_handler(self):
        if not all(k in self.config for k in ('target_board_name', 'target_list_name')):
            raise AssertionError(
                "'target_board_name' and 'target_list_name' should both be present")
        self.list_id = None
        self.labels = None
        self.time_format = self.config['submit_time_format']
        self.auth = TrelloAuth(self)

    def upload(self, submissions):
        """
        :type submissions: [submission]
        """
        try:
            with requests.Session() as s:
                s.auth = self.auth
                self.session = s

                if self.list_id is None:
                    board_id = self.find_id_by_name('/member/me/boards',
                                                    self.config['target_board_name'])
                    labels = self.get('/boards/{}/labels', board_id, field='name')
                    self.labels = {d['name'].lower(): d['id'] for d in labels}
                    self.list_id = self.find_id_by_name('/boards/{}/lists',
                                                        self.config['target_list_name'],
                                                        board_id)

                # TODO check duplications
                for sub in submissions:
                    print(sub.oj, self.labels[sub.oj])
                    date = sub.submit_time.strftime(self.time_format)
                    self.post('/cards',
                              idList=self.list_id,
                              name='{}. {}'.format(sub.problem_id, sub.problem_title),
                              desc='{}\n-- Accepted on {}'.format(sub.problem_url, date),
                              pos='top',
                              due=None,
                              idLabels=self.labels.get(sub.oj, ''))

        except KeyError as e:
            raise ValueError('Target name not found on Trello: {}'.format(e)) from None

    def find_id_by_name(self, path, name, *args, **kwargs):
        items = self.get(path, *args, field='name', **kwargs)
        for d in items:
            if d['name'] == name:
                return d['id']
        raise KeyError(name)

    def get(self, path, *args, **kwargs):
        return self.request('GET', path, args, kwargs)

    def post(self, path, *args, **kwargs):
        self.request('POST', path, args, kwargs)

    def request(self, method, path, args=[], params={}):
        url = 'https://api.trello.com/1' + path.format(*args)
        self.session.params.update(params)
        r = self.session.request(method, url)

        LOGGER.debug('Trello query url: %s, recieving %s and %s', r.url, r.status_code, r.text)
        if r.status_code == 401:
            self.auth.init_user_token()
            return self.request(method, path, args, params)
        if r.status_code >= 400:
            raise RuntimeError('Exception occured when querying {}, recieving {} and {}'
                               .format(r.url, r.status_code, r.text))

        return r.json()

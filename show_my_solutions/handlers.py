import logging
from .dbmanager import add_milestone
from .auth import TrelloAuth
from .utils import WebsiteSession

LOGGER = logging.getLogger(__name__)


class HandlerMeta(type):

    name = 'Handler'
    loaded = {}
    registered = {}

    def __new__(mcs, name, bases, nmspc):
        cls = super().__new__(mcs, name, bases, nmspc)
        cls_name = nmspc['name']
        if cls_name:
            mcs.registered[cls_name] = cls
            LOGGER.info('Register %s: %s', mcs.name, cls_name)
        return cls

    @classmethod
    def get(mcs, name, reactor):
        if name not in mcs.loaded:
            mcs.loaded[name] = mcs.registered[name](reactor)
        return mcs.loaded[name]

build_handler = HandlerMeta.get


class BaseHandler(metaclass=HandlerMeta):

    name = None
    defaults = {}

    def __init__(self, reactor):
        self.reactor = reactor
        self.options = self.reactor.options.get(self.name, {})
        dict(self.defaults)
        self.options.update()
        self.init()
        LOGGER.debug("%s '%s' has inited: %s", type(type(self)).name, self.name, self.options)

    def init(self):
        """Init configuration here."""
        raise NotImplementedError

    def upload(self, submissions):
        raise NotImplementedError


class TrelloHandler(BaseHandler):

    name = 'trello'
    defaults = {
        # Do not display hour and minute because some OJs do not provide that
        'submit_time_format': '%b %d %Z',
        'user_token': None,
        'auth_expiration': '30days',
        'target_board_name': None,
        'target_list_name': None,
    }
    APP_KEY = '7a0445134100faef2f5bbbc4437a42e6'
    API_URL = 'https://api.trello.com/1'

    def init(self):
        assert all(k in self.options for k in ('target_board_name', 'target_list_name')), \
            "'target_board_name' and 'target_list_name' should both be present"
        self.list_id = None
        self.me_id = None
        self.labels = None
        self.auth = TrelloAuth(self)

    def upload(self, submissions):
        """
        :type submissions: [submission]
        """
        done = 0
        try:
            with WebsiteSession(self.API_URL, self.auth, self.auth.init_user_token) as s:
                if self.list_id is None:
                    self.me_id = s.json('/member/me')['id']

                    boards = s.json('/member/me/boards')
                    board_id = self.find_id_by_name(boards, self.options['target_board_name'])

                    NAME_ONLY = {'field': 'name'}
                    labels = s.json('/boards/{}/labels'.format(board_id), params=NAME_ONLY)
                    self.labels = {d['name'].lower(): d['id'] for d in labels}

                    lists = s.json('/boards/{}/lists'.format(board_id), params=NAME_ONLY)
                    self.list_id = self.find_id_by_name(lists, self.options['target_list_name'])

                # TODO check duplications
                for sub in submissions:
                    time_format = self.options.get('submit_time_format',
                                                   self.reactor.options['submit_time_format'])
                    date = sub.submit_time.strftime(time_format)
                    s.post('/cards', params={
                        'idList': self.list_id,
                        'name': '{}. {}'.format(sub.problem_id, sub.problem_title),
                        'desc': '{}\n-- Accepted on {}'.format(sub.problem_url, date),
                        'pos': 'top',
                        'due': None,
                        'idLabels': self.labels.get(sub.oj, ''),
                        'idMembers': self.me_id,
                    })
                    done += 1

        except KeyError as e:
            raise ValueError('Target name not found on Trello: {}'.format(e)) from None
        finally:
            if done > 0:
                add_milestone(self.name,
                              submissions[done - 1].pid)

    @classmethod
    def find_id_by_name(cls, items, name):
        for d in items:
            if d['name'] == name:
                return d['id']
        raise KeyError(name)

import logging
import itertools
import pytz
from parsedatetime import Calendar
from .dbmanager import get_lastest_problem_id, Submission
from .utils import WebsiteSession

LOGGER = logging.getLogger(__name__)


class ScraperMeta(type):

    name = 'Scraper'
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

build_scraper = ScraperMeta.get


class BaseScraper(metaclass=ScraperMeta):

    name = None
    host = None
    defaults = {}

    def __init__(self, reactor):
        self.reactor = reactor
        self.options = dict(self.defaults)
        self.options.update(self.reactor.options.get(self.name, {}))

        self.session = WebsiteSession(self.host, login=self.login)
        self.login()

        self.init()
        LOGGER.debug("%s '%s' has inited: %s", type(self).name, self.name, self.options)

    def init(self):
        """Init configuration here."""
        raise NotImplementedError

    def login(self):
        """Login to the website to scrape. Will be called when authentication failed

        :return: Whether successful logged in or not
        :rtype: Bool
        """
        return False

    def fetch(self):
        """
        :rtype [Submission]:
        """
        raise NotImplementedError


class LeetCodeScraper(BaseScraper):

    name = 'leetcode'
    host = 'https://leetcode.com'

    def init(self):
        self.cal = Calendar()

    def login(self):
        assert all(k in self.options for k in ('username', 'password')), \
            'Missing username and/or password in config file'

        login_path = '/accounts/login/'
        self.session.get(login_path)
        csrf_token = self.session.cookies['csrftoken']

        r = self.session.post(login_path, {
            'login': self.options['username'],
            'password': self.options['password'],
            'csrfmiddlewaretoken': csrf_token,
        })

        info_incorrect = 'The login and/or password you specified are not correct'
        assert info_incorrect not in r.text, info_incorrect

        del self.options['password']
        return True

    def fetch(self):
        # Fetch a list of all accepted submissions
        main_soup = self.session.soup('/problemset/algorithms/')
        ac_dict = {}
        for row in main_soup.select('#problemList > tbody > tr'):
            ac, prob_id, title_path, _, _, _, _ = row('td')
            if ac.span['class'] != ['ac']:
                continue
            title, path = title_path.a.string, title_path.a['href']
            ac_dict[path] = [prob_id, title, None]

        self.fetch_submit_times_by(ac_dict, get_lastest_problem_id(self.name))

        # Refine data
        return [
            Submission(self.name,
                       prob_id.string,
                       title,
                       self.session(path),
                       # TODO possible get tz of user submission?
                       self.cal.parseDT(self.normalize_time(ago.string), tzinfo=pytz.utc)[0])
            for path, (prob_id, title, ago) in ac_dict.items()
        ]

    def fetch_submit_times_by(self, ac_dict, latest_id=-1):
        for i in itertools.count(1):
            sub_soup = self.session.soup('/submissions/{}'.format(i))
            rows = sub_soup.select('#result_testcases > tbody > tr')
            if not rows:
                break
            for row in rows:
                ago, title_path, status, _, _ = row('td')
                if 'status-accepted' not in status.a['class']:
                    continue
                sub = ac_dict[title_path.a['href']]
                if sub[0] == latest_id:
                    return
                sub[-1] = ago

    def normalize_time(self, date):
        words = date.split(',')
        for i in range(len(words) - 1):
            words[i] += ' ago'
        return ','.join(words)

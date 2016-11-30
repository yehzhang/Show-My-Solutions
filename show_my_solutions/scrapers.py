import logging
import itertools
import pytz
from parsedatetime import Calendar
from tzlocal import get_localzone
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
    tzinfo = None
    defaults = {}

    CAL = Calendar()

    def __init__(self, reactor):
        self.reactor = reactor
        self.options = dict(self.defaults)
        self.options.update(self.reactor.options.get(self.name, {}))

        assert self.tzinfo, 'Timezone missing'

        self.session = WebsiteSession(self.host, login=self.login)

        self.init()
        self.login()
        LOGGER.debug("%s '%s' has inited: %s", type(type(self)).name, self.name, self.options)

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

    def parse_datetime(self, s):
        return self.CAL.parseDT(s, tzinfo=self.tzinfo)[0]


class LeetCodeScraper(BaseScraper):

    name = 'leetcode'
    host = 'https://leetcode.com'
    tzinfo = get_localzone()
    defaults = {
        'username': None,
        'password': None,
    }

    def init(self):
        self.username, self.password = map(self.options.get, ('username', 'password'))
        assert self.username and self.password, 'Missing username and/or password in config file'

    def login(self):
        login_path = '/accounts/login/'
        self.session.get(login_path)
        csrf_token = self.session.cookies['csrftoken']

        r = self.session.post(login_path, data={
            'login': self.username,
            'password': self.password,
            'csrfmiddlewaretoken': csrf_token,
        })

        info_incorrect = 'The login and/or password you specified are not correct'
        assert info_incorrect not in r.text, info_incorrect

        return True

    def fetch(self):
        # Fetch a list of all accepted submissions
        main_soup = self.session.soup('/problemset/algorithms/')
        ac_dict = {}
        for row in main_soup.select('#problemList > tbody > tr'):
            ac, prob_id, title_path, _, _, _, _ = row('td')
            if ac.span['class'] != ['ac']:
                continue
            title = title_path.a
            path = title['href']
            ac_dict[path] = [prob_id, title, None]

        self.fetch_submit_times_by(ac_dict, get_lastest_problem_id(self.name))

        # Refine data
        def normalize_time(date):
            words = date.split(',')
            for i in range(len(words) - 1):
                words[i] += ' ago'
            return ','.join(words)

        return [
            Submission(self.name,
                       prob_id.string,
                       title.string,
                       self.session(path),
                       self.parse_datetime(normalize_time(ago.string)))
            for path, (prob_id, title, ago) in ac_dict.items() if ago is not None
        ]

    def fetch_submit_times_by(self, ac_dict, latest_id=None):
        for i in itertools.count(1):
            sub_soup = self.session.soup('/submissions/{}'.format(i))
            rows = sub_soup.select('#result-testcases > tbody > tr')
            if not rows:
                if i == 1:
                    LOGGER.warning('Not found any submissions at all')
                break
            for row in rows:
                ago, title_path, status, _, _ = row('td')
                if 'status-accepted' not in status.a['class']:
                    continue
                sub = ac_dict[title_path.a['href']]
                if sub[0].string == latest_id:
                    return
                sub[-1] = ago


class POJScraper(BaseScraper):

    name = 'poj'
    host = 'http://poj.org'
    tzinfo = pytz.timezone('Asia/Shanghai')
    defaults = {
        'username': None,
    }

    def init(self):
        self.user_id = self.options['username']
        assert self.user_id, 'Username missing'

    def fetch(self):
        ac_dict = {}

        latest_id = get_lastest_problem_id(self.name)
        soup = self.session.soup('/status', params={
            'user_id': self.user_id,
            'result': 0,
        })
        while True:
            rows = soup.select('body > table:nth-of-type(2) > tr')[1:]
            if not rows:
                break

            for row in rows:
                _, _, prob_id, _, _, _, _, _, time = row('td')
                prob_id = prob_id.a.string
                if prob_id == latest_id:
                    break
                ac_dict[prob_id] = time  # assert sorted by submit_time

            next_page = soup.select('body > p:nth-of-type(2) > a')[-1]['href']
            soup = self.session.soup(next_page)

        return [
            Submission(self.name,
                       prob_id,
                       self.session.soup(
                           '/problem', params={'id': prob_id}).select('div.ptt')[0].string,
                       self.session.last_url,
                       self.parse_datetime(time.string))
            for prob_id, time in ac_dict.items()
        ]

import logging
import requests
from bs4 import BeautifulSoup
from . import __title__ as title, __version__ as version

LOGGER = logging.getLogger(__name__)


class WebsiteSession(requests.Session):

    def __init__(self, prefix, auth=None, login=None):
        """
        :param prefix: protocal and domain to prepend in all requests
        :param auth: same as the one in requests.Session
        :param login: called when authentication failed
        """
        super().__init__()
        self.prefix = prefix
        if auth:
            self.auth = auth
        self.headers.update({
            'Referer': prefix,
            'User-Agent': '{} {}'.format(title, version),
        })
        self.login = login
        self.last_url = None

    def __del__(self):
        self.close()

    def __call__(self, path):
        return self.prepend_host(path)

    def request(self, method, url, *args, **kwargs):
        url = self.prepend_host(url)
        r = super().request(method, url, *args, **kwargs)
        self.last_url = r.url

        LOGGER.debug('Query url: %s with method %s, recieving status code %s',
                     r.url, method, r.status_code)
        if r.status_code == 401:
            if not kwargs.get('is_retry') and self.login:
                if self.login():
                    return self.request(method, url, *args, **kwargs, is_retry=True)
            raise RuntimeError('Cannot authenticate. Please try again')
        if r.status_code >= 400:
            raise RuntimeError('Exception occured: {}'.format(r.text))

        return r

    def soup(self, *args, **kwargs):
        r = self.get(*args, **kwargs)
        return BeautifulSoup(r.text, 'html.parser')

    def json(self, *args, **kwargs):
        return self.get(*args, **kwargs).json()

    def prepend_host(self, path):
        return '/'.join([self.prefix, path.lstrip('/')])

import logging
import socket
from threading import Timer, Thread
from urllib.parse import urlencode
from webbrowser import open_new
from requests.auth import AuthBase
from flask import Flask, render_template, request
from .dbmanager import fetch_user_token, save_user_token


LOGGER = logging.getLogger(__name__)


def gen_url_query(url, sep='?', **kwargs):
    return sep.join([url, urlencode(kwargs)])


class TrelloAuth(AuthBase):

    ip = '127.0.0.1'
    port = 25345

    def __init__(self, handler):
        self.handler = handler
        self.address = (self.ip, self.port)
        self.port += 1
        self.key, self.token = map(handler.config.get, ('app_key', 'user_token'))
        if self.token is None:
            self.token = fetch_user_token(handler.name)
            if self.token is None:
                self.init_user_token()

    def __call__(self, r):
        r.prepare_url(r.url, {'key': self.key, 'token': self.token})
        return r

    def init_user_token(self):
        self.ask_user_for_token()
        save_user_token(self.handler.name, self.token)
        
    def ask_user_for_token(self):
        LOGGER.info('Please authenticate me in the browser')
        LOGGER.info('Press Ctrl+C to quit in case of frozen')
        auth_url = gen_url_query('https://trello.com/1/authorize',
                                 key=self.key,
                                 expiration=self.handler.config['auth_expiration'],
                                 scope='read,write',
                                 name=self.handler.settings['app_name'],
                                 callback_method='fragment',
                                 return_url='http://{}:{}'.format(*self.address)
                                 )
        open_new(auth_url)
        return self.listen_for_token()

    def listen_for_token(self):
        app = Flask(__name__)

        @app.route('/')
        def first():
            return render_template('fragment.html', title='Redirecting...')

        @app.route('/fragment/<token>')
        def second(token):
            LOGGER.debug('Token fetched: %s', token)
            self.token = token
            return shutdown(True)

        @app.route('/<path:success>')
        def shutdown(success):
            shutdown_server()
            if success is True:
                message = 'Login succeeds. You may close this window now.'
            else:
                message = 'Login failed. You may close this window and restart the script.'
            return render_template('shutdown.html', message=message, title='Finished')

        def shutdown_server():
            LOGGER.debug('Stop listening for token')
            func = request.environ.get('werkzeug.server.shutdown')
            if func is None:
                raise RuntimeError('Cannot stop listening: Flask is not running with the Werkzeug Server')
            func()

        self.token = None
        app.run(*self.address)
        LOGGER.debug('Listening stopped')
        if self.token is None:
            raise ValueError('Failed to fetch the access token')
        return self.token

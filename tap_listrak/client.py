from datetime import datetime, timedelta

import backoff
import requests
from singer import metrics

class Server5xxError(Exception):
    pass

class ListrakClient(object):
    BASE_URL = 'https://api.listrak.com/email/v1'

    def __init__(self, client_id, client_secret):
        self.__client_id = client_id
        self.__client_secret = client_secret
        self.__token = None
        self.__expires = None
        self.__session = requests.Session()

    def __enter__(self):
        self.get_token()
        return self

    def __exit__(self, type, value, traceback):
        self.__session.close()

    @backoff.on_exception(backoff.expo,
                          Server5xxError,
                          max_tries=5,
                          factor=2)
    def get_token(self):
        if self.__token is not None and self.__expires > datetime.utcnow():
            return

        response = self.__session.post(
            'https://auth.listrak.com/OAuth2/Token',
            data={
                'grant_type': 'client_credentials',
                'client_id': self.__client_id,
                'client_secret': self.__client_secret
            })

        if response.status_code >= 500:
            raise Server5xxError()

        if response.status_code != 200:
            raise Exception('Unable to authenticate - Ensure your credentials are correct' +
                            ' and this IP has been whitelisted in the Listrak admin panel.')

        data = response.json()

        self.__token = data['access_token']

        expires_seconds = data['expires_in'] - 10 # pad by 10 seconds
        self.__expires = datetime.utcnow() + timedelta(seconds=expires_seconds)

    @backoff.on_exception(backoff.expo,
                          Server5xxError,
                          max_tries=5,
                          factor=2)
    def request(self, method, path, **kwargs):
        self.get_token()

        if 'endpoint' in kwargs:
            endpoint = kwargs['endpoint']
            del kwargs['endpoint']
        else:
            endpoint = None

        if 'headers' not in kwargs:
            kwargs['headers'] = {}
        kwargs['headers']['Authorization'] = 'Bearer {}'.format(self.__token)

        with metrics.http_request_timer(endpoint) as timer:
            response = self.__session.request(method, self.BASE_URL + path, **kwargs)
            timer.tags[metrics.Tag.http_status_code] = response.status_code

        if response.status_code >= 500:
            raise Server5xxError()

        response.raise_for_status()
        data = response.json()
        return data['data'], data.get('nextPageCursor')

    def get(self, path, **kwargs):
        return self.request('GET', path, **kwargs)

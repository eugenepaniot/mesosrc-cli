import logging
import os
import requests
from requests.auth import HTTPBasicAuth

from mesosrc.utils.core import merge_two_dicts, truncate_string


class HTTPRequest(object):
    headers = []

    def __init__(self, address, user, password, headers=None, logger=None):
        super(HTTPRequest, self).__init__()

        self.password = password
        self.address = address
        self.user = user

        if logger is None:
            self.logger = logging.getLogger(__name__)
        else:
            self.logger = logger

        if headers is None:
            headers = dict()

        self.headers = headers
        self.session = requests.Session()

    def getHttpSession(self):
        return self.session

    def getAddress(self):
        self.logger.debug("address: %s" % self.address)
        return self.address

    def getUser(self):
        self.logger.debug("user: %s" % self.user)
        return self.user

    def getPassword(self):
        self.logger.debug("password: %s" % self.password)
        return self.password

    def getHeaders(self):
        self.logger.debug("headers: %s" % self.headers)
        return self.headers

    def getURL(self, path):
        url = "%s%s" % (self.getAddress(), os.path.normpath(path))

        self.logger.debug("URL: " + url)
        return url

    def mkRequest(self, path, method=u'GET', headers=None, **kwargs):
        def response_hook(resp, *args, **kwargs):
            self.logger.debug("Response for URL: \"%s\", request method: \"%s\""
                              "\nresponse code: \"%d\", "
                              "\nrequest headers: \"%s\", "
                              "\nresponse headers: \"%s\", "
                              "\nrequest data: \"%s\","
                              "\nresponse data: \"%s\""
                              % (resp.url, resp.request.method,
                                 resp.status_code,
                                 resp.request.headers,
                                 resp.headers,
                                 resp.request.body,
                                 truncate_string(resp.text),
                                 ))
            resp.raise_for_status()

        url = self.getURL(path)
        return requests.Request(url=url,
                                method=method,
                                auth=HTTPBasicAuth(self.getUser(), self.getPassword()),
                                headers=merge_two_dicts(self.getHeaders(), headers),
                                hooks={'response': response_hook},
                                **kwargs
                                ).prepare()

    def PATCH(self, path, data, contentType='application/json'):
        return self.getHttpSession().send(request=self.mkRequest(path, method=u'PATCH',
                                                                 headers={'Content-Type': contentType},
                                                                 data=data
                                                                 )).text

    def DELETE(self, path, data=None, contentType='application/json'):
        return self.getHttpSession().send(request=self.mkRequest(path, method=u'DELETE',
                                                                 headers={'Content-Type': contentType},
                                                                 data=data
                                                                 )).text

    def POST(self, path, data, contentType='application/json'):
        return self.getHttpSession().send(request=self.mkRequest(path, method=u'POST',
                                                                 headers={'Content-Type': contentType},
                                                                 data=data
                                                                 )).text

    def GET(self, path, headers=None):
        return self.getHttpSession().send(request=self.mkRequest(path, headers=headers)).text

    def urlOpenJsonToObject(self, path):
        return self.getHttpSession().send(request=self.mkRequest(path, headers=dict(Accept='application/json'))).json()

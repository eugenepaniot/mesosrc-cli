import base64
import json
import logging
import ssl
import urllib2

import os

from mesosrc.utils.core import truncate_string


class HTTPRequest(object):
    useAuth = False

    def __init__(self, address, user=None, password=None, headers=None, logger=None):
        super(HTTPRequest, self).__init__()

        if headers is None:
            headers = {}

        self.setLogger(logger)
        self.setAddress(address)
        self.setUser(user)
        self.setPassword(password)
        self.setHeaders(headers)

        if self.user is not None and self.user is not "":
            self.useAuth = True

        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE

        opener = urllib2.build_opener(
            urllib2.HTTPRedirectHandler(),
            urllib2.HTTPHandler(debuglevel=0),
            urllib2.HTTPSHandler(debuglevel=0,
                                 context=context)
        )

        urllib2.install_opener(opener)

    def setLogger(self, logger):
        if logger is None:
            self.logger = logging.getLogger(__name__)
        else:
            self.logger = logger

    def getAddress(self):
        self.logger.debug("address: " + self.address)
        return self.address

    def setAddress(self, address):
        self.address = address

    def getUser(self):
        self.logger.debug("user: " + self.user)
        return self.user

    def setUser(self, user):
        self.user = user

    def getPassword(self):
        self.logger.debug("password: " + self.password)
        return self.password

    def setPassword(self, password):
        self.password = password

    def getHeaders(self):
        self.logger.debug("headers: %s" % self.headers)
        return self.headers

    def setHeaders(self, headers):
        self.headers = headers

    def getAuthBasic(self):
        base64string = base64.b64encode('%s:%s' % (self.getUser(), self.getPassword()))
        return "Basic %s" % base64string

    def getURL(self, path):
        url = "%s%s" % (self.getAddress(), os.path.normpath(path))

        self.logger.debug("URL: " + url)
        return url

    def mkRequest(self, path, data=None, contentType=None, method="GET"):
        url = self.getURL(path)

        if data:
            self.logger.debug("DATA: %s" % data)
            request = urllib2.Request(url, data=data, headers=self.getHeaders())
        else:
            request = urllib2.Request(url, headers=self.getHeaders())

        if self.useAuth:
            request.add_header("Authorization", self.getAuthBasic())

        if contentType:
            request.add_header("Content-Type", contentType)

        request.get_method = lambda: method

        return request

    def PATCH(self, path, data):
        request = self.mkRequest(path=path, data=data, contentType='application/json', method='PATCH')
        return self.urlOpen(req=request)

    def DELETE(self, path, data=None):
        request = self.mkRequest(path=path, data=data, contentType='application/json', method='DELETE')
        return self.urlOpen(req=request)

    def POST(self, path, data):
        request = self.mkRequest(path=path, data=data, contentType='application/json', method='POST')
        return self.urlOpen(req=request)

    def urlOpen(self, uri=None, req=None):
        if uri is not None and req is None:
            req = self.mkRequest(path=uri)

        data = urllib2.urlopen(req).read()

        self.logger.debug("urlOpen data: %s" % data)

        if data:
            self.logger.debug("Data: \"%s\"" % truncate_string(data, lenght=4096))

        return data

    def urlOpenJsonToObject(self, uri):
        return json.loads(self.urlOpen(uri=uri))

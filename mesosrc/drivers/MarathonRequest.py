from time import sleep

import requests

try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse

from pprint import pformat

from mesosrc.drivers.HTTPRequest import HTTPRequest
from mesosrc.utils.exceptions import MaxTriesExceeded, OperatorActionRequired


class MarathonRequest(HTTPRequest):
    leader = None
    scheme = None

    def __init__(self, address, user, password, headers, logger):
        super(MarathonRequest, self).__init__(address, user, password, headers, logger)

        self.scheme = urlparse(self.getAddress()).scheme
        try:
            self.leader = self.urlOpenJsonToObject("/v2/leader")['leader']
        except requests.exceptions.HTTPError as e:
            self.logger.warning("HTTPError response code \"%d\" : \"%s\" for request url: \"%s\" headers: \"%s\"" %
                                (e.response.status_code,
                                 e.response.text,
                                 e.request.url,
                                 e.request.headers
                                 ))
        except Exception as e:
            self.logger.warning("Can't initialize marathon leader from address %s: %s" % (self.getAddress(), repr(e)))
            self.leader = None
        else:
            self.logger.debug("Marathon leader successful initialized")

    def getAddress(self):
        if self.leader:
            self.logger.debug("leader: " + self.leader)
            return "%s://%s" % (self.scheme, self.leader)
        else:
            self.logger.debug("address: " + self.address)
            return self.address

    def getApps(self):
        return self.urlOpenJsonToObject("/v2/apps?embed=apps.tasks")

    def getAppByID(self, id):
        assert id, "Task ID is required"

        return self.urlOpenJsonToObject("/v2/apps/%s?embed=apps.tasks" % id)

    def getQueue(self):
        return self.urlOpenJsonToObject("/v2/queue")

    def getDeployments(self):
        return self.urlOpenJsonToObject("/v2/deployments")

    def getDeploymentByID(self, id):
        assert id, "Deployment ID is required"

        return list(filter(lambda x: 'id' in x and x['id'] == id, self.urlOpenJsonToObject("/v2/deployments")))

    def awaitForDeploymentID(self, id, maxTries=1800, rollBack=True):
        assert id, "Deployment ID is required"

        try:
            currentTries = 0
            while self.getDeploymentByID(id):
                if currentTries > maxTries:
                    raise MaxTriesExceeded(currentTries, maxTries, "Deployment %s failed" % id)

                currentTries += 1
                if currentTries % 3 == 0:
                    self.logger.info("Awaiting(%d/%d) for deploymentId: %s" % (currentTries, maxTries,
                                                                               id))
                sleep(1)

        except MaxTriesExceeded as e:
            self.logger.error("Queue: %s" % pformat(self.getQueue()))
            rollbackDeploymentId = None

            if rollBack:
                self.logger.info("Going to rollback deploymentId: %s" % id)
                rollbackDeploymentId = self.DELETE(path="/v2/deployments/%s" % id, data=None)

            raise OperatorActionRequired("%s. Rollback deployment Id: %s" % (e.message, rollbackDeploymentId))

from mesosrc.drivers.HTTPRequest import HTTPRequest


class MesosMasterRequest(HTTPRequest):
    def getSlaves(self):
        return self.urlOpenJsonToObject("/slaves")

    def getSlaveByID(self, id):
        assert id, "ID is required"
        return self.urlOpenJsonToObject("/slaves?slave_id=%s" % id)

    def getTasks(self):
        return self.urlOpenJsonToObject("/tasks")

    def getTaskByID(self, id):
        assert id, "ID is required"
        return self.urlOpenJsonToObject("/tasks?task_id=%s" % id)

    def getStateSummary(self):
        return self.urlOpenJsonToObject("/state-summary")

    def getMaintenanceStatus(self):
        return self.urlOpenJsonToObject("/maintenance/status")

    def getMaintenanceSchedule(self):
        return self.urlOpenJsonToObject("/maintenance/schedule")

    def getMaintenanceScheduleByHostname(self, hostname):
        assert hostname, "hostname required"

        maint = self.getMaintenanceStatus()

        if 'draining_machines' not in maint:
            return

        return list(filter(lambda x: x['id']['hostname'] == hostname, maint['draining_machines']))


class MesosSlaveRequest(HTTPRequest):
    def getState(self):
        return self.urlOpenJsonToObject("/state")

    def readFile(self, file, offset=0):
        return self.urlOpenJsonToObject(path="/files/read?path=%s&offset=%u" % (file, offset))

    def browseFiles(self, dir):
        return self.urlOpenJsonToObject(path="/files/browse?path=%s" % dir)
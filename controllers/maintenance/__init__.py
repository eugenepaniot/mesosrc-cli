from cement.core.controller import expose, CementBaseController

from utils.core import format_nanos, sec2time
from utils.exceptions import WrongDataException


class MesosMaintenanceBaseController(CementBaseController):
    class Meta:
        label = 'maintenanceBase'
        stacked_on = 'base'
        stacked_type = 'nested'

        description = "Maintenance primitives"
        aliases = ['maintenance', 'maint']
        aliases_only = True

    @expose(hide=True)
    def default(self):
        self.status()
        self.schedule()

    @expose(help="Retrieves the maintenance status of the cluster", aliases=['st'])
    def status(self):
        def pretty(data):
            ret = {"In drain": [], "Down machines": []}

            for i in data:
                if i == 'down_machines':
                    self.app.log.debug(data[i])
                    for d in data[i]:
                        ret["Down machines"].append("%s (%s)" % (d["hostname"], d["ip"]))

                elif i == 'draining_machines':
                    self.app.log.debug("draining_machines")
                    for d in data[i]:
                        ret["In drain"].append("%s (%s)" % (d["id"]["hostname"], d["id"]["ip"]))
                else:
                    raise WrongDataException(data)

            return ret

        self.app.render(pretty(self.app.mesos.getMaintenanceStatus()), headers="keys", tablefmt="fancy_grid")

    @expose(help="A list of maintenance windows", aliases=['sc'])
    def schedule(self):
        def pretty(data):
            if 'windows' not in data:
                yield {"Machine": None, "Schedule Window": None}
            else:
                for w in data['windows']:
                    un = w['unavailability']
                    self.app.log.debug(un)

                    for m in w['machine_ids']:
                        self.app.log.debug(m)
                        self.app.log.debug(un['duration']['nanoseconds'])

                        yield {
                            "Machine": "%s (%s)" % (m['hostname'], m['ip']),
                            "Schedule Window": "Start time: %s, duration: %s. End time: %s" % (
                                format_nanos(un['start']['nanoseconds']),
                                sec2time(int(un['duration']['nanoseconds']/1e9)),
                                format_nanos(un['start']['nanoseconds'] + un['duration']['nanoseconds'])
                            )
                        }

        self.app.render(pretty(self.app.mesos.getMaintenanceSchedule()), headers="keys", tablefmt="fancy_grid")

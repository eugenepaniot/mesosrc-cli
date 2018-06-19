import json

import datetime
import socket

from cement.core.controller import expose, CementBaseController
from mesosrc.utils.exceptions import UnexpectedBehaviour


class MesosMaintenanceUpdateController(CementBaseController):
    class Meta:
        label = 'MesosMaintenanceController'
        stacked_on = 'maintenanceBase'
        stacked_type = 'nested'

        description = 'Updates the cluster\'s maintenance schedule'

        aliases = ['update', 'upd', 'up']
        aliases_only = True

        arguments = [
            (['--id'], dict(help='Machines affected by this maintenance window')),
            (['--hostame'], dict(help='Hostames of affected machines. Else discover by ID')),
            (['--start'], dict(help='The start time which machines is expected to be down.\n'
                                    'In format: "%%Y-%%m-%%d %%H:%%M" (default: "%(default)s") (now). ',
                               default=datetime.datetime.now().strftime("%Y-%m-%d %H:%M"))),
            (['--duration'], dict(help='Interval (hours) during which this set of machines is expected to be down '
                                       '(default: %(default)sh)', type=int,
                                  default=24)),
        ]

    @expose(hide=True)
    def default(self):
        self.app.args.parse_args(['--help'])

    @expose(help="Remove cluster's maintenance schedules", aliases=['unsc', 'usc'])
    def unschedule(self):
        response = self.app.mesos.POST(path="/maintenance/schedule", data="{}")
        if response:
            self.app.log.info(response)


    @expose(help="Update cluster's maintenance schedule", aliases=['sc'])
    def schedule(self):
        def getMachineByID(id):
            data = self.app.mesos.getSlaveByID(id)
            assert data['slaves'], "slaves key exists and not empty"

            return {"hostname": data['slaves'][0]['hostname'],
                    "ip": socket.gethostbyname(data['slaves'][0]['hostname'])}

        if self.app.pargs.hostame is not None and self.app.pargs.id is not None:
            self.app.log.error("--id conflicted argument")
            self.app.args.parse_args(['--help'])
            return

        if self.app.pargs.id is None and self.app.pargs.hostame is None:
            self.app.log.error("--id or --hostame required arguments")
            self.app.args.parse_args(['--help'])
            return

        # if self.app.pargs.id is None:
        #     self.app.log.error("--id required arguments")
        #     self.app.args.parse_args(['--help'])
        #     return

        start = int(datetime.datetime.strptime(self.app.pargs.start, "%Y-%m-%d %H:%M").strftime('%s')) * 1000000000
        duration = self.app.pargs.duration * 3600 * 1000000000

        schedule = {
            "windows": [{
                "machine_ids": [],
                "unavailability": {
                    "start": {"nanoseconds": start},
                    "duration": {"nanoseconds": duration}
                }
            }]
        }

        if self.app.pargs.id is not None:
            for id in self.app.pargs.id.split(","):
                schedule["windows"][0]["machine_ids"].append(getMachineByID(id=id))
        elif self.app.pargs.hostame is not None:
            for h in self.app.pargs.hostame.split(","):
                schedule["windows"][0]["machine_ids"].append({
                        "hostname": h,
                        "ip": socket.gethostbyname(h)
                    })
        else:
            raise UnexpectedBehaviour()

        response = self.app.mesos.POST(path="/maintenance/schedule", data=json.dumps(schedule))
        if response:
            self.app.log.info(response)

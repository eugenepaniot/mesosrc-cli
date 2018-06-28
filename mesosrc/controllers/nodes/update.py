import json
import socket

from cement.core.controller import expose
from cement.ext.ext_argparse import ArgparseController

from mesosrc.utils.core import to_bool
from mesosrc.utils.exceptions import OperatorActionRequired


class MesosNodesController(ArgparseController):
    class Meta:
        label = 'MesosNodesController'
        stacked_on = 'mesosNodesBase'

        aliases = ['update', 'upd']
        aliases_only = True

        arguments = [
            (['--hostame'], dict(help='Hosts affected')),
            (['--id'], dict(help='IDs affected')),
            (['--force'], dict(help='If set to false then the deployment is canceled and a new '
                                    'deployment is created to revert the changes of this deployment. Without '
                                    'concurrent deployments, this restores the configuration before this deployment. '
                                    'If set to true, then the deployment is still canceled but no rollback deployment '
                                    'is created.'
                                    'Warning - Using force=true to abort a deployment can leave behind '
                                    'unaccounted for tasks and/or leave the app in a mixed state of old and new '
                                    'versions of tasks! \n'
                                    'Default: %(default)s)', type=to_bool, default=False)),
            (['--wait'], dict(help='Await for marathon tasks to complete(default: %(default)s)', type=to_bool,
                              default=True)),
            (['--waitTime'], dict(help='Awaiting time for marathon tasks to complete(default: %(default)ssec)',
                                  type=int, default=3600)),
        ]

    @expose(help="Brings a set of machines back up")
    def up(self):
        if self.app.pargs.hostame is None:
            self.app.log.error("--hostame required argument")
            self.app.args.parse_args(['--help'])

        machines = []

        for h in self.app.pargs.hostame.split(","):
            machines.append(dict(hostname=h, ip=socket.gethostbyname(h)))

        response = self.app.mesos.POST(path="/machine/up", data=json.dumps(machines))
        if response:
            self.app.log.info(response)

    @expose(help="Brings a set of machines down")
    def down(self):
        if self.app.pargs.hostame is None:
            self.app.log.error("--hostame required argument")
            self.app.args.parse_args(['--help'])
            return

        machines = []

        for h in self.app.pargs.hostame.split(","):
            machines.append(dict(hostname=h, ip=socket.gethostbyname(h)))

        response = self.app.mesos.POST(path="/machine/down", data=json.dumps(machines))
        if response:
            self.app.log.info(response)

    @expose(help="Brings a set of machines safe-down. Scale UP and then scale DOWN marathon applications",
            aliases=["safedown", "sdown"])
    def safe_down(self):
        if self.app.pargs.hostame is None:
            self.app.log.error("--hostame required argument")
            self.app.args.parse_args(['--help'])

        appsToPatch = []
        tasksPerHost = {}

        apps = self.app.marathon.getApps()
        for h in self.app.pargs.hostame.split(","):
            if not self.app.mesos.getMaintenanceScheduleByHostname(h):
                raise OperatorActionRequired("You should schedule maintenance for the '%s' node first" % h)

            for a in apps:
                for t in a['tasks']:
                    if t['host'] == h:
                        if a['id'] not in tasksPerHost:
                            tasksPerHost[a['id']] = dict(instances=a['instances'], hosts=[])

                        if h not in tasksPerHost[a['id']]['hosts']:
                            tasksPerHost[a['id']]['hosts'].append(h)

                        tasksPerHost[a['id']]['instances'] += 1

        for a, s in tasksPerHost.items():
            appsToPatch.append(dict(id=a, instances=s['instances']))

        if not appsToPatch:
            self.app.log.warning("There are no any apps associated to hosts %s" % self.app.pargs.hostame)
            return

        self.app.log.info("Scale-UP apps: %s" % appsToPatch)
        response = self.app.marathon.PATCH(path="/v2/apps?force=%s" % self.app.pargs.force, data=json.dumps(appsToPatch))
        self.app.log.info(response)
        deploymentId = json.loads(response)['deploymentId']

        if to_bool(self.app.pargs.wait):
            self.app.marathon.awaitForDeploymentID(id=deploymentId, maxTries=self.app.pargs.waitTime, rollBack=True)
        else:
            self.app.log.warning("Skip to wait for marathon tasks to complete")

        for a, s in tasksPerHost.items():
            for h in s['hosts']:
                self.app.log.info("Scale-DOWN app %s on host %s" % (a, h))
                response = self.app.marathon.DELETE(path="/v2/apps/%s/tasks?host=%s&scale=true&wipe=false" % (a, h))
                self.app.log.info(response)
                deploymentId = json.loads(response)['deploymentId']

                if to_bool(self.app.pargs.wait):
                    self.app.marathon.awaitForDeploymentID(id=deploymentId, maxTries=self.app.pargs.waitTime,
                                                           rollBack=True)
                else:
                    self.app.log.warning("Skip to wait for marathon tasks to complete")

    # http://mesos.apache.org/documentation/latest/operator-http-api/
    @expose(help="Shutdown the agent and send TASK_GONE_BY_OPERATOR updates for all the running tasks")
    def gone(self):
        if self.app.pargs.id is None:
            self.app.log.error("--id required argument for this action")
            self.app.args.parse_args(['--help'])

        for id in self.app.pargs.id.split(","):
            gone = {
                "type": "MARK_AGENT_GONE",
                "mark_agent_gone": {
                    "agent_id": {
                        "value": id
                    }
                }
            }

            response = self.app.mesos.POST(path="/api/v1", data=json.dumps(gone))
            if response:
                self.app.log.info(response)

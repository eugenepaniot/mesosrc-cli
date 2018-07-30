import pprint
import uuid
from collections import defaultdict, OrderedDict
from time import sleep

import gc
from cement.core.exc import CaughtSignal
from colored import fg, attr

from cement.core.controller import expose, CementBaseController
from requests import HTTPError

from mesosrc.drivers.MesosRequest import MesosSlaveRequest
from mesosrc.utils.core import to_bool


class MesosTasksBase(CementBaseController):
    class Meta:
        label = 'mesosTasksBase'
        stacked_on = 'base'
        stacked_type = 'nested'

        description = "Mesos Tasks primitives"
        aliases = ['tasks', 'task']
        aliases_only = True

        arguments = [
            (['--app'], dict(help='Marathon application')),
            (['--colored'], dict(help='Enable colored output in logs (default: %(default)s)', type=to_bool,
                                 default=True)),
            (['--last'], dict(help='Tail only on the latest logs lines(default: %(default)s)', type=to_bool,
                              default=True)),
        ]

    @expose(hide=True)
    def default(self):
        self.list()

    @expose(help="List tasks for Marathon application")
    def list(self):
        def pretty(tasks):
            for task in tasks:
                ret = {
                    "App ID": task['appId'],
                    "Task ID": task['id'].split(".")[1],
                    "State": task['state'],
                    "Health-check results": ', '.join("%s" % "Healthy" if hc['alive'] else "Unhealthy"
                                                      for hc in task['healthCheckResults']),
                    "Host": task['host'],
                    "ipAddresses": ', '.join("%s" % i['ipAddress'] for i in task['ipAddresses']),
                    "Ports": ', '.join("%d" % p for p in task['ports']),
                }

                yield OrderedDict((k, ret[k])
                                  for k in ['App ID', 'Task ID',
                                            'Host',
                                            'ipAddresses',
                                            'Ports',
                                            'Health-check results'
                                            ])

        if self.app.pargs.app is None:
            self.app.log.error("--app required arguments")
            self.app.args.parse_args(['--help'])

        data = self.app.marathon.getAppByID(self.app.pargs.app)

        self.app.render(pretty(data['tasks']), headers="keys", tablefmt="fancy_grid")

    @expose(help="Connect to slaves to gather application's stdout/stderr logs", aliases=['log'])
    def logs(self):
        def getColor(id, source=None):
            if self.app.pargs.colored:
                u = str(uuid.uuid3(uuid.NAMESPACE_OID, str(id)))
                return fg(int(u.replace('-', ''), 36) % 256) + attr('bold') if source == "stderr" else 0
            else:
                return attr('reset')

        def getDirectoryByID(state, id):
            self.app.log.debug("Id: %s" % id)

            for framework in state['frameworks']:
                for executor in framework['executors']:
                    if executor['id'] == id:
                        return executor['directory']

        if self.app.pargs.app is None:
            self.app.log.error("--app required argument")
            self.app.args.parse_args(['--help'])

        readedFiles = defaultdict(int)
        while True:
            try:
                for task in self.app.marathon.getAppByID(self.app.pargs.app)['tasks']:
                    mesos_task = self.app.mesos.getTaskByID(task['id'])['tasks'][0]

                    # Get info about mesos slave in case of non-standard port
                    slave = self.app.mesos.getSlaveByID(mesos_task['slave_id'])

                    slaveRequest = MesosSlaveRequest(address="http://%s:%d" % (slave['hostname'],
                                                                               slave['port'] if 'port' in slave
                                                                               else 5051),
                                                     logger=self.app.log)

                    slaveState = slaveRequest.getState()
                    directoryPerExecutor = getDirectoryByID(slaveState, mesos_task['id'])

                    if not directoryPerExecutor:
                        self.app.log.warning("There are no directoryPerExecutor")
                        continue

                    for f in ['stderr', 'stdout']:
                        try:
                            file = "%s/%s" % (directoryPerExecutor, f)

                            if file not in readedFiles:
                                self.app.log.info("Reading %s from %s" % (file, slaveRequest.getAddress()))

                                if self.app.pargs.last:
                                    browseFiles = slaveRequest.browseFiles(dir=directoryPerExecutor)
                                    fileFound = list(filter(lambda x: x['path'] == file, browseFiles))
                                    if fileFound:
                                        readedFiles[file] = fileFound[0]['size']

                            while True:
                                data = slaveRequest.readFile(file=file, offset=readedFiles.get(file, 0))['data']
                                if not data:
                                    break

                                print(u"%s%s | %s | %s | %s %s\n" %
                                      (
                                          getColor(mesos_task['id'], f),
                                          f, mesos_task['id'], slave['hostname'], data,
                                          attr('reset')
                                      ))

                                readedFiles[file] += len(data)

                        except HTTPError as e:
                            self.app.log.warning(pprint.pformat(e))
                            continue

            except (CaughtSignal, KeyboardInterrupt) as e:
                raise e
            except Exception as e:
                self.app.log.warning(pprint.pformat(e))

            gc.collect()
            sleep(1)

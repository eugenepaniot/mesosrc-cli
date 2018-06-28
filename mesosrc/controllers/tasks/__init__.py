import pprint
import uuid
from collections import defaultdict
from time import sleep

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
        self.app.log.warning("TBD")

    @expose()
    def attach(self):
        self.app.log.warning("TBD")

    @expose(help="Connect to slaves to gather application's stdout/stderr logs", aliases=['log'])
    def logs(self):
        def getColor(id, source=None):
            add = ''
            if source == "stderr":
                add = attr('bold')

            if self.app.pargs.colored:
                u = str(uuid.uuid3(uuid.NAMESPACE_OID, str(id)))
                return fg(int(u.replace('-', ''), 36) % 256) + add
            else:
                return attr('reset')

        def getDirectoryByID(state, id):
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

                    slave = self.app.mesos.getSlaveByID(mesos_task['slave_id'])['slaves'][0]

                    slaveRequest = MesosSlaveRequest(address="http://%s:%d" % (slave['hostname'], slave['port']),
                                                     logger=self.app.log)

                    slaveState = slaveRequest.getState()
                    directoryPerExecutor = getDirectoryByID(slaveState, mesos_task['id'])
                    if not directoryPerExecutor:
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

            sleep(1)

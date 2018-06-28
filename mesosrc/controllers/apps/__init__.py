from collections import OrderedDict, defaultdict
from cement.core.controller import expose, CementBaseController


class MarathonAppsBase(CementBaseController):
    class Meta:
        label = 'marathonAppsBase'
        stacked_on = 'base'
        stacked_type = 'nested'

        description = "Marathon apps primitives"
        aliases = ['apps', 'app']
        aliases_only = True

        arguments = [
            (['--app'], dict(help='Filter applications by marathon application id')),
            (['--node'], dict(help='Filter applications by node hostname')),
        ]

    @expose(hide=True)
    def default(self):
        self.list()

    @expose(help="Get the list of running applications")
    def list(self):
        def pretty(data):
            for app in data:
                tasksPerSlaves = defaultdict(int)

                for t in app['tasks']:
                    tasksPerSlaves[t['host']] += 1

                ret = {
                    "ID": app['id'],
                    "Instances": app['instances'],
                    "Tasks running, staging": "%d/%d" % (app['tasksRunning'], app['tasksStaged']),
                    "Tasks healthy, unhealthy": "%d/%d" % (app['tasksHealthy'], app['tasksUnhealthy']),
                    "Tasks per slaves": '\n'.join("%s: %d" % (k, v) for (k, v) in tasksPerSlaves.items())
                }

                yield OrderedDict((k, ret[k])
                                  for k in ['ID', 'Instances',
                                            'Tasks running, staging',
                                            'Tasks healthy, unhealthy',
                                            'Tasks per slaves'
                                            ])

        if self.app.pargs.app:
            data = [self.app.marathon.getAppByID(self.app.pargs.app)]
        elif self.app.pargs.node:
            data = [a for a in self.app.marathon.getAppsByNode(self.app.pargs.node)]
        else:
            data = self.app.marathon.getApps()

        self.app.render(pretty(data), headers="keys", tablefmt="fancy_grid")

    @expose(help="List all the tasks queued up or waiting to be scheduled", aliases=['q'])
    def queue(self):
        def pretty(queue):
            for q in queue:
                ret = dict(ID=q['app']['id'], rejectSummaryLaunchAttempt="")

                for r in q['processedOffersSummary']['rejectSummaryLaunchAttempt']:
                    if r['declined'] > 0:
                        ret['rejectSummaryLaunchAttempt'] = ret['rejectSummaryLaunchAttempt'] \
                                                            + "%s: %d\n" % (r['reason'], r['declined'])

                yield OrderedDict((k, ret[k]) for k in ['ID', 'rejectSummaryLaunchAttempt'])

        self.app.render(pretty(self.app.marathon.getQueue()['queue']), headers="keys", tablefmt="fancy_grid")

    @expose(help="List all running deployments", aliases=['deployment', 'deploy', 'd'])
    def deployments(self):
        def pretty(deployments):
            for d in deployments:
                for a in d['currentActions']:
                    ret = dict(App=a['app'], Action=a['action'],
                               CurrentStep=d['currentStep'], TotalSteps=d['totalSteps'])

                    yield OrderedDict((k, ret[k]) for k in ['App', 'Action', 'CurrentStep', 'TotalSteps'])

        self.app.render(pretty(self.app.marathon.getDeployments()), headers="keys", tablefmt="fancy_grid")

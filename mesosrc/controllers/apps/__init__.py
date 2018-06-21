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

    @expose(hide=True)
    def default(self):
        self.list()

    @expose(help="Get the list of running applications")
    def list(self):
        def pretty(data):
            if 'apps' not in data:
                return

            for a in data['apps']:
                tasksPerSlaves = defaultdict(int)

                for t in a['tasks']:
                    tasksPerSlaves[t['host']] += 1

                ret = {
                    "ID": a['id'],
                    "Instances": a['instances'],
                    "Tasks staging, running": "%d/%d" % (a['tasksStaged'], a['tasksRunning']),
                    "Tasks per slaves": '\n'.join("%s: %d" % (k, v) for (k, v) in tasksPerSlaves.items())
                }

                yield OrderedDict((k, ret[k])
                                  for k in ['ID', 'Instances',
                                            'Tasks staging, running',
                                            'Tasks per slaves']
                                  )

        self.app.render(pretty(self.app.marathon.getApps()), headers="keys", tablefmt="fancy_grid")

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

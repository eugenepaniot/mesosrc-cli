from collections import OrderedDict
from cement.core.controller import expose, CementBaseController


class MesosNodeBaseController(CementBaseController):
    class Meta:
        label = 'mesosNodesBase'
        stacked_on = 'base'
        stacked_type = 'nested'

        description = "Mesos Nodes primitives"
        aliases = ['nodes', 'node']
        aliases_only = True

    @expose(hide=True)
    def default(self):
        self.list()

    @expose(help="Information about agents")
    def list(self):
        def pretty(data):
            def get_resources(res):
                self.app.log.debug("res: %s" % res)

                ret = {"CPU": "NA", "Memory": "NA"}
                for r in res:
                    if r['name'] == "mem":
                        ret['Memory'] = int(r['scalar']['value']) >> 10
                    elif r['name'] == "cpus":
                        ret['CPU'] = r['scalar']['value']

                return ret

            if 'slaves' not in data:
                return

            state_summary = self.app.mesos.getStateSummary()

            for d in data['slaves']:
                res_total = get_resources(d['unreserved_resources_full'])
                res_used = get_resources(d['used_resources_full'])

                ss_slave = filter(lambda x: 'id' in x and x['id'] == d['id'],
                                  state_summary['slaves'])[0]

                ret = {
                    "ID": d['id'],
                    "Hostname": d['hostname'],
                    "Mesos Version / CCS Version": "%s/%s" % (d['version'], d['attributes'].get("ccs_version", "NA")),
                    "CPU (Total/Used)": "%s/%s" % (res_total['CPU'], res_used['CPU']),
                    "Memory (Total/Used),\nGB": "%s/%s" % (res_total['Memory'], res_used['Memory']),
                    "Tasks\nstaging, running, failed": "%d/%d/%d" % (ss_slave['TASK_STAGING'],
                                                                     ss_slave['TASK_RUNNING'],
                                                                     ss_slave['TASK_FAILED']
                                                                     )
                }

                yield OrderedDict((k, ret[k])
                                  for k in ['ID', 'Hostname', 'CPU (Total/Used)',
                                            'Memory (Total/Used),\nGB', 'Mesos Version / CCS Version',
                                            'Tasks\nstaging, running, failed']
                                  )

        self.app.render(pretty(self.app.mesos.getSlaves()), headers="keys", tablefmt="fancy_grid")

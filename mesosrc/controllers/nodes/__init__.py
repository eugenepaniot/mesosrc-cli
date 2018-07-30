from collections import OrderedDict
from cement.core.controller import expose, CementBaseController

from mesosrc.utils.core import percentage


class MesosNodeBaseController(CementBaseController):
    class Meta:
        label = 'mesosNodesBase'
        stacked_on = 'base'
        stacked_type = 'nested'

        description = "Mesos Nodes primitives"
        aliases = ['nodes', 'node', 'slave', 'slaves', 'agent', 'agents']
        aliases_only = True

    @expose(hide=True)
    def default(self):
        self.list()

    @expose(help="Information about agents")
    def list(self):
        def pretty(data):
            def get_resources_full(res):
                ret = {"CPU": "NA", "Memory": "NA"}
                for r in res:
                    if 'name' in r:
                        if r['name'] == "mem":
                            ret['Memory'] = int(r['scalar']['value']) >> 10
                        elif r['name'] == "cpus":
                            ret['CPU'] = r['scalar']['value']
                    else:
                        if r == "mem":
                            ret['Memory'] = int(res[r]) >> 10
                        elif r == "cpus":
                            ret['CPU'] = res[r]

                return ret

            if 'slaves' not in data:
                return

            state_summary = self.app.mesos.getStateSummary()

            for d in data['slaves']:
                res_total = get_resources_full(d['unreserved_resources_full'] if 'unreserved_resources_full' in d
                                               else d['unreserved_resources'])
                res_used = get_resources_full(d['used_resources_full'] if 'used_resources_full' in d
                                              else d['used_resources'])

                ss_slave = list(filter(lambda x: 'id' in x and x['id'] == d['id'],
                                       state_summary['slaves']))[0]

                ret = {
                    "Hostname": d['hostname'],
                    "Mesos Version/CCS Version": "%s/%s" % (d['version'], d['attributes'].get("ccs_version", "NA")),
                    "CPU (Total/Used, Used %)": "%s/%s, %.1f%%" % (res_total['CPU'], res_used['CPU'],
                                                                   percentage(res_used['CPU'], res_total['CPU'])),
                    "Memory (Total/Used, Used %),\nGB": "%s/%s, %.1f%%" % (res_total['Memory'], res_used['Memory'],
                                                                           percentage(res_used['Memory'],
                                                                                      res_total['Memory'])),
                    "Tasks\nstaging/running/failed": "%d/%d/%d" % (ss_slave['TASK_STAGING'],
                                                                   ss_slave['TASK_RUNNING'],
                                                                   ss_slave['TASK_FAILED']
                                                                   )
                }

                yield OrderedDict((k, ret[k])
                                  for k in ['Hostname', 'CPU (Total/Used, Used %)',
                                            'Memory (Total/Used, Used %),\nGB', 'Mesos Version/CCS Version',
                                            'Tasks\nstaging/running/failed']
                                  )

        self.app.render(pretty(self.app.mesos.getSlaves()), headers="keys", tablefmt="fancy_grid")

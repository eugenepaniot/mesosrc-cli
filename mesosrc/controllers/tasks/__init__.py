from cement.core.controller import expose, CementBaseController


class MesosTasksBase(CementBaseController):
    class Meta:
        label = 'mesosTasksBase'
        stacked_on = 'base'
        stacked_type = 'nested'

        description = "Mesos Tasks primitives"
        aliases = ['tasks', 'task']
        aliases_only = True

    @expose(hide=True)
    def default(self):
        self.app.log.warn("TBD")

    @expose()
    def attach(self):
        self.app.log.warn("TBD")

    @expose()
    def logs(self):
        self.app.log.warn("TBD")


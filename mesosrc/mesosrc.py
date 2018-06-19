#!/usr/bin/env python
# coding=utf-8

import os
import traceback
import urllib2
import sys

from cement.core.exc import CaughtSignal
from cement.ext.ext_colorlog import ColorLogHandler
from cement.core.controller import CementBaseController, expose
from cement.core.foundation import CementApp
from cement.utils.misc import init_defaults

from controllers.apps import MarathonAppsBase
from controllers.maintenance import MesosMaintenanceBaseController
from controllers.maintenance.update import MesosMaintenanceUpdateController
from controllers.nodes import MesosNodeBaseController
from controllers.nodes.update import MesosNodesController
from controllers.tasks import MesosTasksBase
from drivers.MarathonRequest import MarathonRequest
from drivers.MesosRequest import MesosRequest
from utils.exceptions import OperatorActionRequired

app_config = init_defaults('drivers-cli', 'log.colorlog', 'mesos', 'marathon')

app_config['log.colorlog'] = dict()
app_config['log.colorlog']['colorize_console_log'] = True

app_config['mesos'] = dict()
app_config['mesos']['host'] = "http://localhost:5050"
app_config['mesos']['user'] = None
app_config['mesos']['password'] = None

app_config['marathon'] = dict()
app_config['marathon']['host'] = "http://localhost:8080"
app_config['marathon']['user'] = "marathon"
app_config['marathon']['password'] = "marathon"


def pre_close(app):
    app.log.info("Exit with code %d: %s" % (app.exit_code, os.strerror(app.exit_code)))


def extendAppWMesosMarathon(app):
    try:
        mesos = MesosRequest(address=app.config.get('mesos', 'host'),
                             user=app.config.get('mesos', 'user'),
                             password=app.config.get('mesos', 'password'),
                             headers=None,
                             logger=app.log,
                             )

        marathon = MarathonRequest(address=app.config.get('marathon', 'host'),
                                   user=app.config.get('marathon', 'user'),
                                   password=app.config.get('marathon', 'password'),
                                   headers=None,
                                   logger=app.log,
                                   )

        app.extend('mesos', mesos)
        app.extend('marathon', marathon)
    except Exception, e:
        raise OperatorActionRequired("Can't initialize Mesos or Marathon connections. %s" % repr(e))

class MesosLogHandler(ColorLogHandler):
    class Meta:
        label = 'MesosLogHandler'

        config_defaults = dict(
            file=None,
            level='DBEUG',
            to_console=True,
            rotate=False,
            max_bytes=512000,
            max_files=4,
            colorize_file_log=False,
            colorize_console_log=True,
        )


class MesosBaseController(CementBaseController):
    class Meta:
        label = 'base'
        description = "Mesos RingCentral CLI"
        stacked_on = 'base'

    @expose(hide=True)
    def default(self):
        self.app.args.parse_args(['--help'])


class MesosRCCli(CementApp):
    class Meta:
        label = 'mesosrc'
        config_defaults = app_config
        arguments_override_config = True
        exit_on_close = True
        extensions = ['colorlog', 'tabulate']
        output_handler = 'tabulate'
        log_handler = MesosLogHandler
        base_controller = MesosBaseController

        handlers = [
            MesosLogHandler
        ]

        handlers += [
            MesosMaintenanceBaseController
            , MesosMaintenanceUpdateController
            , MesosNodeBaseController
            , MesosNodesController
            , MesosTasksBase
            , MarathonAppsBase
        ]

        config_files = [
            "mesosrc.conf",
            "/etc/mesosrc/mesosrc.conf",
            "~/.mesosrc.conf",
            "~/mesosrc.conf",
            "~/.mesosrc/mesosrc.conf",
            "~/.mesosrc/config",
            "~/.local/etc/mesosrc/mesosrc.conf",
        ]

        hooks = [
            ('pre_run', extendAppWMesosMarathon),
            ('pre_close', pre_close),
        ]


def main():
    with MesosRCCli() as app:
        try:
            app.setup()
            app.run()
            app.close()

        except CaughtSignal, e:
            app.log.error("interrupted")
            sys.exit(3)

        except urllib2.HTTPError as e:
            app.log.error("Code: \"%s\" reason: \"%s\" data: \"%s\"" % (e.code, e.reason, e.read()))
            sys.exit(1)

        except OperatorActionRequired, e:
            app.log.error(e.message)
            sys.exit(1)

        except Exception, e:
            exc_type, exc_value, exc_traceback = sys.exc_info()

            app.log.fatal(traceback.extract_tb(exc_traceback))

            app.log.error(e.message)
            sys.exit(1)

            # app.render({
            #     "Error message": [e.message],
            #     "Traceback": [ exc_traceback.tb_frame ]
            # }, headers="keys", tablefmt="fancy_grid")

if __name__ == "__main__":
    main()
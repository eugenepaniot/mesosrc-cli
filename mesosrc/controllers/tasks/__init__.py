import json
import logging
import pprint

import tornado

from colored import fg, attr

from cement.core.exc import CaughtSignal
from cement.core.controller import expose, CementBaseController

from mesosrc.utils.core import to_bool

from tornado import ioloop, gen, httpclient
from tornado.httpclient import AsyncHTTPClient

from mesosrc.utils.exceptions import OperatorActionRequired


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
            (['--colored'], dict(help='Enable colored output for logspout (default: %(default)s)', type=to_bool,
                                 default=True)),
        ]

    @expose(hide=True)
    def default(self):
        self.app.log.warning("TBD")

    @expose()
    def attach(self):
        self.app.log.warning("TBD")

    @expose(help="Connect to logspout to gather application's stdout/stderr", aliases=['logsp'])
    def logspout(self):
        @gen.coroutine
        def getLogs():
            global strbuff
            strbuff = str()

            self.app.log.info("Streaming logs from: %s" % urls)

            def on_chunk(chunk):
                def getColor(id, source):
                    add=''
                    if source == "stderr":
                        add = attr('bold')

                    if self.app.pargs.colored:
                        return fg(int(id, 36) % 256) + add
                    else:
                        return attr('reset')

                global strbuff
                try:
                    resp = json.loads(strbuff + chunk.decode())
                except ValueError:
                    strbuff += chunk.decode()
                else:
                    strbuff = str()

                    print("%s%s | %s | %s %s\n" %
                          (
                              getColor(resp['Container']['Id'][:16], resp['Source']),
                              resp['Source'],
                              resp['Container']['Id'][:16],
                              resp['Data'].strip(),
                              attr('reset')
                          ))

            waiter = gen.WaitIterator(*[
                AsyncHTTPClient(force_instance=True).fetch(
                    httpclient.HTTPRequest(url=url,
                                           streaming_callback=on_chunk,
                                           request_timeout=0,
                                           headers={'Accept': 'application/json'}
                                           ))
                for url in urls
            ])

            while not waiter.done():
                response = yield waiter.next()

        if self.app.pargs.app is None:
            self.app.log.error("--app required argument")
            self.app.args.parse_args(['--help'])

        urls = []

        for task in self.app.marathon.getAppByID(self.app.pargs.app)['app']['tasks']:
            mesos_task = self.app.mesos.getTaskByID(task['id'])['tasks'][0]
            container_id = mesos_task['statuses'][0]['container_status']['container_id']['value']

            container_name = "mesos-%s" % container_id

            urls.append("http://%s/logs/name:%s" % (task['host'], container_name))

        lh = self.app.handler.get('log', 'logging')

        logging.getLogger('tornado').addHandler(lh)
        logging.getLogger("tornado.general").addHandler(lh)
        logging.getLogger("tornado.access").addHandler(lh)
        logging.getLogger("tornado.application").addHandler(lh)

        try:
            tornado.ioloop.IOLoop.instance().run_sync(getLogs)
        except CaughtSignal as e:
            raise e
        except Exception as e:
            tornado.ioloop.IOLoop.instance().stop()
            raise OperatorActionRequired("Error: %s" % pprint.pformat(e))

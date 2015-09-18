import datetime as dt

import socket

from twisted.web import resource, static
from twisted.application.service import IServiceCollection

from scrapy.utils.misc import load_object

from .interfaces import IPoller, IEggStorage, ISpiderScheduler

from urlparse import urlparse

class Root(resource.Resource):

    def __init__(self, config, app):
        resource.Resource.__init__(self)
        self.debug = config.getboolean('debug', False)
        self.runner = config.get('runner')
        datadir = config.get('data_dir')
        logsdir = config.get('logs_dir')
        itemsdir = config.get('items_dir')
        local_items = itemsdir and (urlparse(itemsdir).scheme.lower() in ['', 'file'])
        self.app = app
        self.nodename = config.get('node_name', socket.gethostname())
        self.putChild('', Home(self, local_items))

        self.putChild('spiders', Spiders(self, local_items))

        if datadir:
            self.putChild('data', static.File(datadir, 'text/plain'))
        if logsdir:
            self.putChild('logs', static.File(logsdir, 'text/plain'))
        if local_items:
            self.putChild('items', static.File(itemsdir, 'text/plain'))
        self.putChild('jobs', Jobs(self, local_items))
        services = config.items('services', ())
        for servName, servClsName in services:
          servCls = load_object(servClsName)
          self.putChild(servName, servCls(self))
        self.update_projects()

    def update_projects(self):
        self.poller.update_projects()
        self.scheduler.update_projects()

    @property
    def launcher(self):
        app = IServiceCollection(self.app, self.app)
        return app.getServiceNamed('launcher')

    @property
    def scheduler(self):
        return self.app.getComponent(ISpiderScheduler)

    @property
    def eggstorage(self):
        return self.app.getComponent(IEggStorage)

    @property
    def poller(self):
        return self.app.getComponent(IPoller)


class Home(resource.Resource):

    def __init__(self, root, local_items):
        resource.Resource.__init__(self)
        self.root = root
        self.local_items = local_items

    def render_GET(self, txrequest):
        vars = {
            'projects': ', '.join(self.root.scheduler.list_projects()),
        }
        s = """
<html>
<head><title>Scrapyd</title></head>
<body>
<h1>Scrapyd</h1>
<p>Available projects: <b>%(projects)s</b></p>
<ul>
<li><a href="/spiders">Spiders</a></li>
<li><a href="/jobs">Jobs</a></li>
""" % vars
        if self.local_items:
            s += '<li><a href="/items/">Items</a></li>'
        s += """
<li><a href="/logs/">Logs</a></li>
<li><a href="http://scrapyd.readthedocs.org/en/latest/">Documentation</a></li>
</ul>

<h2>How to schedule a spider?</h2>

<p>To schedule a spider you need to use the API (this web UI is only for
monitoring)</p>

<p>Example using <a href="http://curl.haxx.se/">curl</a>:</p>
<p><code>curl http://localhost:6800/schedule.json -d project=default -d spider=somespider</code></p>

<p>For more information about the API, see the <a href="http://scrapyd.readthedocs.org/en/latest/">Scrapyd documentation</a></p>
</body>
</html>
""" % vars
        return s

class Jobs(resource.Resource):

    def __init__(self, root, local_items):
        resource.Resource.__init__(self)
        self.root = root
        self.local_items = local_items

    def render(self, txrequest):
        cols = 6
        s = "<html><head><title>Scrapyd</title></head>"
        s += "<body>"
        s += "<h1>Jobs</h1>"
        s += "<p><a href='..'>Go back</a></p>"
        s += "<table border='1'>"
        s += "<tr><th>Project</th><th>Spider</th><th>Job</th><th>PID</th><th>Runtime</th><th>Log</th>"
        if self.local_items:
            s += "<th>Items</th>"
            cols = 7
        s += "</tr>"
        s += "<tr><th colspan='%s' style='background-color: #ddd'>Pending</th></tr>" % cols
        for project, queue in self.root.poller.queues.items():
            for m in queue.list():
                s += "<tr>"
                s += "<td>%s</td>" % project
                s += "<td>%s</td>" % str(m['name'])
                s += "<td>%s</td>" % str(m['_job'])
                s += "</tr>"
        s += "<tr><th colspan='%s' style='background-color: #ddd'>Running</th></tr>" % cols
        for p in self.root.launcher.processes.values():
            s += "<tr>"
            for a in ['project', 'spider', 'job', 'pid']:
                s += "<td>%s</td>" % getattr(p, a)
            s += "<td>%s</td>" % (dt.datetime.now() - p.start_time)
            s += "<td><a href='/logs/%s/%s/%s.log'>Log</a></td>" % (p.project, p.spider, p.job)
            if self.local_items:
                s += "<td><a href='/items/%s/%s/%s.jl'>Items</a></td>" % (p.project, p.spider, p.job)
            s += "</tr>"
        s += "<tr><th colspan='%s' style='background-color: #ddd'>Finished</th></tr>" % cols
        for p in self.root.launcher.finished:
            s += "<tr>"
            for a in ['project', 'spider', 'job']:
                s += "<td>%s</td>" % getattr(p, a)
            s += "<td></td>"
            s += "<td>%s</td>" % (p.end_time - p.start_time)
            s += "<td><a href='/logs/%s/%s/%s.log'>Log</a></td>" % (p.project, p.spider, p.job)
            if self.local_items:
                s += "<td><a href='/items/%s/%s/%s.jl'>Items</a></td>" % (p.project, p.spider, p.job)
            s += "</tr>"
        s += "</table>"
        s += "</body>"
        s += "</html>"
        return s

from .utils import get_spider_list
from .config import Config
from apscheduler.schedulers.twisted import TwistedScheduler
import logging
import os

class Spiders(resource.Resource):
    u"""
    显示Spider的工作状态以及调度计划
    """
    config = Config()
    def __init__(self, root, local_items):
        resource.Resource.__init__(self)
        self.root = root
        self.local_items = local_items
        self.spider_status_dic = {}

        logging.basicConfig()
        self.scheduler = TwistedScheduler()
        self.scheduler.start()

    def get_spider_status(self, project):
        spider_status = self.spider_status_dic.get(project)
        if not spider_status:
            spider_status = dict((spider, {'status': 'finished', 'timestamp': None, 'job': None, 'schedule_job': None})
                                 for spider in get_spider_list(project))
            self.spider_status_dic[project] = spider_status
        self._update_spider_status(project)
        return spider_status

    def _update_spider_status(self, project):
        u"""
        先获取目前任务调度情况，然后再获取apsheduler中的任务。
        """
        spider_status = self.spider_status_dic.get(project)
        for project, queue in self.root.poller.queues.items():
            for m in queue.list():
                spider = m['name']
                job = m['_job']
                spider_status[spider]['status'] = 'pending'
                spider_status[spider]['timestamp'] = None
                spider_status[spider]['job'] = job
        for p in self.root.launcher.processes.values():
            spider = p.spider
            spider_status[spider]['status'] = 'running'
            spider_status[spider]['timestamp'] = p.start_time
            spider_status[spider]['job'] = p.job
        for p in self.root.launcher.finished:
            spider = p.spider
            spider_status[spider]['status'] = 'finished'
            spider_status[spider]['timestamp'] = p.end_time
            spider_status[spider]['job'] = p.job

        for spider in spider_status:
            status = spider_status[spider]
            sjob = self.scheduler.get_job(spider)
            status['schedule_job'] = sjob
            if sjob:
                status['next_time'] = sjob.next_run_time
            else:
                status['next_time'] = None
            # sjob._get_run_times()

    def render_GET(self, txrequest):
        args = dict((k, v[0]) for k, v in txrequest.args.items())
        project = args.pop('project', 'careerTalk')
        spider_status = self.get_spider_status(project)
        content = "<tr>"
        for th in ['spider', 'status', 'timestamp', 'next_time', 'data']:
            content += "<th>%s</th>" % th
        content += "</tr>"
        for spider in spider_status:
            status = spider_status[spider]
            content += "<tr>"
            content += "<td>%s</td><td>%s</td><td>%s</td><td>%s</td>" \
                       % (spider, status['status'], status['timestamp'], status['next_time'])
            content += "<td><a href='/data/%s/'>data</a></td>" % spider
            content += "</tr>"
        sub_form = "<form action='' method='post'><input type='submit' value='开启所有任务'></input></form>"
        html = "<table>"+content+"</table>"+sub_form
        return html

    def render_POST(self, txrequest):
        args = dict((k, v[0]) for k, v in txrequest.args.items())
        project = args.pop('project', 'careerTalk')
        # spiders = ['NJU', 'BIT', 'ECUST', 'RUC']
        spiders = get_spider_list(project)

        tstart = dt.datetime.utcnow()
        for spider in spiders:
            job = self.scheduler.add_job(spider_crawl, 'interval', minutes=60, replace_existing=True,
                                         id=spider, next_run_time=tstart, args=[project, spider])
            tstart = tstart + dt.timedelta(seconds=5)
        return "<span>任务全部开启</span><a href='/'>返回</a>"


def spider_crawl(project, spider):
    # print project, spider
    http_port = Spiders.config.getint('http_port', 6800)
    bind_address = Spiders.config.get('bind_address', '0.0.0.0')

    url = 'http://%s:%s/schedule.json' % (bind_address, http_port)
    cmd = "curl %s -d project=%s -d spider=%s" % (url, project, spider)
    logging.info('start apscheduler_job: '+cmd)
    os.system(cmd)


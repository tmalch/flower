from __future__ import absolute_import

import sys
import copy
import logging
import itertools
try:
    from itertools import imap
except ImportError:
    imap = map

from tornado import web

from ..views import BaseHandler
from ..utils.tasks import iter_tasks, get_task_by_id, as_dict

logger = logging.getLogger(__name__)
from celery.events.state import Task
from celery.result import AsyncResult

class TaskView(BaseHandler):
    @web.authenticated
    def get(self, task_id):
        task = get_task_by_id(self.application.events, task_id)

        if task is None:
            raise web.HTTPError(404, "Unknown task '%s'" % task_id)

        self.render("task.html", task=task)


class TasksDataTable(BaseHandler):
    parents = {}

    @web.authenticated
    def get(self):
        app = self.application
        draw = self.get_argument('draw', type=int)
        start = self.get_argument('start', type=int)
        length = self.get_argument('length', type=int)
        search = self.get_argument('search[value]', type=str)

        column = self.get_argument('order[0][column]', type=int)
        sort_by = self.get_argument('columns[%s][data]' % column, type=str)
        sort_order = self.get_argument('order[0][dir]', type=str) == 'asc'
        do_grouping = self.get_argument('grouping', type=bool)

        def key(item):
            val = getattr(item[1], sort_by)
            if sys.version_info[0] == 3:
                val = str(val)
            return val

        def add_hierarchy(args):
            uuid, task = args
            if not hasattr(task, "hierarchy"):
                hierarchy = ascendants(task.uuid)
                task.hierarchy = hierarchy
                task._fields = task._fields + ('hierarchy',)
            return uuid, task

        def ascendants(uuid):
            asc = [uuid]
            while uuid != self.parents.get(uuid, uuid):
                asc.append(self.parents.get(uuid, uuid))
                uuid = self.parents.get(uuid, uuid)
            return asc

        tasks = iter_tasks(app.events, search=search)
        if do_grouping:
            for uuid, task in iter_tasks(app.events):
                aresult = AsyncResult(uuid)
                for child in aresult.children:
                    self.parents[child.id] = uuid
            tasks = map(add_hierarchy, tasks)

            groups = []
            tasks = sorted(tasks, key=lambda args: args[1].hierarchy[-1])
            for root_uuid, group in itertools.groupby(tasks, key=lambda args: args[1].hierarchy[-1]):
                groups.append(sorted(group, key=lambda args: len(args[1].hierarchy)))

            # sort groups by root element
            groups = sorted(groups, key=lambda group: key(group[0]), reverse=sort_order)
            tasks = itertools.chain.from_iterable(groups)
        else:
            tasks = sorted(tasks, key=key, reverse=sort_order)
        tasks = list(map(self.format_task, tasks))

        filtered_tasks = []
        i = 0
        for _, task in tasks:
            if i < start:
                i += 1
                continue
            if i >= (start + length):
                break
            task = as_dict(task)
            if 'hierarchy' in task:
                task['hierarchy'] = "{}_{}".format(task['hierarchy'][-1], len(task['hierarchy']) - 1)
            if task['worker']:
                task['worker'] = task['worker'].hostname
            filtered_tasks.append(task)
            i += 1

        self.write(dict(draw=draw, data=filtered_tasks,
                        recordsTotal=len(tasks),
                        recordsFiltered=len(tasks)))

    def format_task(self, args):
        uuid, task = args
        custom_format_task = self.application.options.format_task

        if custom_format_task:
            try:
                task = custom_format_task(copy.copy(task))
            except:
                logger.exception("Failed to format '%s' task", uuid)
        return uuid, task


class TasksView(BaseHandler):
    @web.authenticated
    def get(self):
        app = self.application
        capp = self.application.capp

        time = 'natural-time' if app.options.natural_time else 'time'
        if capp.conf.CELERY_TIMEZONE:
            time += '-' + capp.conf.CELERY_TIMEZONE

        self.render(
            "tasks.html",
            tasks=[],
            columns=app.options.tasks_columns,
            time=time,
        )

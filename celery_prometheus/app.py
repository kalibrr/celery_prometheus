# -*- encoding: utf-8 -*-

from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals
from __future__ import division

import argparse
from functools import wraps
import time
import logging
import psutil
import six
import re
import subprocess
from celery import Celery
from celery.backends.redis import RedisBackend
from threading import Thread
from prometheus_client import (
    start_http_server,
    Histogram,
    Gauge,
)


parser = argparse.ArgumentParser(description='Listens to Celery events and exports them to Prometheus')

parser.add_argument(
    '--config', '-c',
    dest='config',
    action='store',
    type=six.text_type,
    help='Fully-qualified config module name'
)
parser.add_argument(
    '--host',
    dest='host',
    action='store',
    type=six.text_type,
    default='localhost',
    help='The hostname to listen on'
)
parser.add_argument(
    '--port', '-p',
    dest='port',
    action='store',
    type=int,
    default=9897,
    help='The port to listen on'
)

task_queuetime_seconds = Histogram(
    'celery_task_queuetime_seconds',
    'number of seconds spent in queue for celery tasks',
    ['task_name', 'exchange'],
    buckets=(
        .005, .01, .025, .05, .075, .1, .25, .5,
        .75, 1.0, 2.5, 5.0, 7.5, 10.0, 100.0, float('inf')
    )
)
task_runtime_seconds = Histogram(
    'celery_task_runtime_seconds',
    'number of seconds spent executing celery tasks',
    ['task_name', 'state', 'exchange'],
    buckets=(
        .005, .01, .025, .05, .075, .1, .25, .5,
        .75, 1.0, 2.5, 5.0, 7.5, 10.0, 100.0, float('inf')
    )
)
queue_length = None
queue_rss = None
queues = None


def setup_metrics(app):
    global queue_length
    global queue_rss
    global queues

    if isinstance(app.backend, RedisBackend):
        # queues must be explicitly configured in config
        queues_conf = app.conf.get('CELERY_QUEUES')
        if not queues_conf:
            return
        queues = [queue.name for queue in queues_conf]
        if not queues:
            return

        queue_length = Gauge(
            'celery_queue_length',
            'length of Celery queues',
            ['queue']
        )

        queue_rss = Gauge(
            'celery_queue_rss_megabytes',
            'RSS of celery queue',
            ['queue']
        )

def check_queue_lengths(app):
    while True:
        pipe = app.backend.client.pipeline(transaction=False)
        for queue in queues:
            pipe.llen(queue)
        for result, queue in zip(pipe.execute(), queues):
            queue_length.labels(queue).set(result)
        time.sleep(45)

def check_queue_rss():
    pattern = '\[celeryd: (.*@.*):MainProcess\]'
    while True:
    output = []
        for proc in psutil.process_iter():
            cmd = proc.cmdline()
            if len(cmd):
                token = re.findall(pattern, cmd[0])
                if token:
                    output.append(
                        {
                            'name': token[0].split('@')[0],
                            'rss': proc.memory_info().rss / 1000000.0
                        }
                    )
        for item in output:
            queue_rss.labels(item['name']).set(item['rss'])
        time.sleep(45)

def celery_monitor(app):
    state = app.events.State()

    def task_handler(fn):
        @wraps(fn)
        def wrapper(event):
            state.event(event)
            task = state.tasks.get(event['uuid'])
            if task is None:
                # can't do anything with this
                logging.warning('task for event %s is missing' % event)
                return
            return fn(event, task)
        return wrapper

    @task_handler
    def handle_started_task(event, task):
        if task.sent is not None:
            queue_time = time.time() - task.sent
            task_queuetime_seconds.labels(task.name, task.exchange).observe(queue_time)

    @task_handler
    def handle_succeeded_task(event, task):
        task_runtime_seconds.labels(task.name, 'succeeded', task.exchange).observe(task.runtime)

    @task_handler
    def handle_failed_task(event, task):
        task_runtime_seconds.labels(task.name, 'failed', task.exchange).observe(task.runtime)

    @task_handler
    def handle_retried_task(event, task):
        task_runtime_seconds.labels(task.name, 'retried', task.exchange).observe(task.runtime)

    try_interval = 1
    while True:
        try:
            try_interval *= 2

            with app.connection() as connection:
                recv = app.events.Receiver(connection, handlers={
                    'task-started': handle_started_task,
                    'task-succeeded': handle_succeeded_task,
                    'task-failed': handle_failed_task,
                    'task-retried': handle_retried_task,
                    '*': state.event
                })
                try_interval = 1
                recv.capture(limit=None, timeout=None, wakeup=True)

        except (KeyboardInterrupt, SystemExit):
            try:
                import _thread as thread
            except ImportError:
                import thread
            thread.interrupt_main()
        except Exception as e:
            logging.error("Failed to capture events: '%s', "
                          "trying again in %s seconds.",
                          e, try_interval)
            logging.debug(e, exc_info=True)
            time.sleep(try_interval)


def run():
    args = parser.parse_args()

    app = Celery()
    app.config_from_object(args.config)

    setup_metrics(app)

    start_http_server(args.port, args.host)

    if queue_length is not None:
        Thread(target=check_queue_lengths, args=(app,)).start()

    if queue_rss is not None:
        Thread(target=check_queue_rss).start()

    celery_monitor(app)


if __name__ == '__main__':
    run()

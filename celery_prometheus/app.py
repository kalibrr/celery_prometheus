# -*- encoding: utf-8 -*-

from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals
from __future__ import division

import gevent.monkey
monkey.patch_all()

import argparse
import os
from functools import wraps
import time
import logging
import psutil
import six
import re
from celery import Celery
from threading import Thread
from prometheus_client import (
    start_http_server,
    Histogram,
    Gauge,
    Counter
)


parser = argparse.ArgumentParser(description='Listens to Celery events and exports them to Prometheus')


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
parser.add_argument(
    '--monitor-memory',
    dest='monitor_memory',
    action='store_true',
    default=False,
    help='whether to monitor memory (RSS) of celery workers'
)
parser.add_argument(
    '--broker',
    dest='broker',
    action='store',
    type=six.text_type,
    help='URL to Celery broker'
)
parser.add_argument(
    '--tz', dest='tz',
    help="Timezone used by the celery app."
)
parser.add_argument(
    '--queue',
    dest='queues',
    action='append',
    help='Celery queues to check length for'
)

# We have these counters because sometimes we might not
# be able to find out the queue time or runtime of a task,
# so we can't mark an observation in the histogram
task_submissions = Counter(
    'celery_task_submissions',
    'number of times a task has been submitted',
    ['task_name', 'exchange'],
)
task_completions = Counter(
    'celery_task_completions',
    'number of times a task has been completed',
    ['task_name', 'state', 'exchange'],
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


def setup_metrics(app, monitor_memory=False, queues=None):
    global queue_length
    global queue_rss
    if queues and app.conf.BROKER_URL.startswith('redis:'):
        queue_length = Gauge(
            'celery_queue_length',
            'length of Celery queues',
            ['queue']
        )

    if monitor_memory:
        queue_rss = Gauge(
            'celery_queue_rss_megabytes',
            'RSS of celery queue',
            ['queue']
        )


def check_queue_lengths(app, queues):
    client = app.broker_connection().channel().client
    while True:
        logging.error('Setting queue lengths for %s' % queues)
        pipe = client.pipeline(transaction=False)
        for queue in queues:
            pipe.llen(queue)
        for result, queue in zip(pipe.execute(), queues):
            logging.error('Setting q length: %s - %s' % (queue, result))
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
            logging.error('Received a task: %s %s' % (event, task))
            return fn(event, task)
        return wrapper

    @task_handler
    def handle_started_task(event, task):
        if task is not None and task.sent is not None:
            queue_time = time.time() - task.sent
            task_queuetime_seconds.labels(task.name, task.exchange).observe(queue_time)
            task_submissions.labels(task.name, task.exchange).inc()
        else:
            task_submissions.labels('unknown', 'unknown').inc()

    @task_handler
    def handle_succeeded_task(event, task):
        if task is not None:
            logging.debug('Succeeded a task: %s %s %s', task.name, task.exchange, task.runtime)
            task_runtime_seconds.labels(task.name, 'succeeded', task.exchange).observe(task.runtime)
            task_completions.labels(task.name, 'succeeded', task.exchange).inc()
        else:
            logging.debug('Could not track a succeeded task')
            task_completions.labels('unknown', 'succeeded', 'unknown').inc()

    @task_handler
    def handle_failed_task(event, task):
        if task is not None:
            logging.debug('Failed a task: %s %s %s', task.name, task.exchange, task.runtime)
            task_runtime_seconds.labels(task.name, 'failed', task.exchange).observe(task.runtime)
            task_completions.labels(task.name, 'failed', task.exchange).inc()
        else:
            logging.debug('Could not track a failed task')
            task_completions.labels('unknown', 'failed', 'unknown').inc()

    @task_handler
    def handle_retried_task(event, task):
        if task is not None:
            logging.debug('Retried a task: %s %s %s', task.name, task.exchange, task.runtime)
            task_runtime_seconds.labels(task.name, 'retried', task.exchange).observe(task.runtime)
            task_completions.labels(task.name, 'retried', task.exchange).inc()
        else:
            logging.debug('Could not track a retried task')
            task_completions.labels('unknown', 'retried', 'unknown').inc()

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
    if args.tz:
        os.environ['TZ'] = args.tz
        time.tzset()

    app = Celery(broker=args.broker)

    setup_metrics(app, monitor_memory=args.monitor_memory, queues=args.queues)

    start_http_server(args.port, args.host)

    if queue_length is not None:
        Thread(target=check_queue_lengths, args=(app, args.queues)).start()

    if queue_rss is not None:
        Thread(target=check_queue_rss).start()

    celery_monitor(app)


if __name__ == '__main__':
    run()

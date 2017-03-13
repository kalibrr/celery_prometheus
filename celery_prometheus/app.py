# -*- encoding: utf-8 -*-

from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals
from __future__ import division

import argparse
from functools import wraps
import time
import logging

import six
from celery import Celery
from prometheus_client import (
    start_http_server,
    Histogram,
    ProcessCollector,
    PROCESS_COLLECTOR,
    REGISTRY
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
parser.add_argument(
    '--prefix',
    dest='prefix',
    action='store',
    type=six.text_type,
    default='celery',
    help='Prefix for exported metrics'
)

task_queuetime_seconds = None
task_runtime_seconds = None


def setup_metrics(prefix):
    global task_queuetime_seconds
    global task_runtime_seconds
    task_queuetime_seconds = Histogram(
        '%s_task_queuetime_seconds' % prefix,
        'number of seconds spent in queue for celery tasks',
        ['task_name', 'exchange'],
        buckets=(
            .005, .01, .025, .05, .075, .1, .25, .5,
            .75, 1.0, 2.5, 5.0, 7.5, 10.0, 100.0, float('inf')
        )
    )
    task_runtime_seconds = Histogram(
        '%s_task_runtime_seconds' % prefix,
        'number of seconds spent executing celery tasks',
        ['task_name', 'state', 'exchange'],
        buckets=(
            .005, .01, .025, .05, .075, .1, .25, .5,
            .75, 1.0, 2.5, 5.0, 7.5, 10.0, 100.0, float('inf')
        )
    )

    # use custom process collector so we have it namespaced properly
    REGISTRY.unregister(PROCESS_COLLECTOR)
    ProcessCollector(prefix)


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

    with app.connection() as connection:
        recv = app.events.Receiver(connection, handlers={
            'task-started': handle_started_task,
            'task-succeeded': handle_succeeded_task,
            'task-failed': handle_failed_task,
            'task-retried': handle_retried_task,
            '*': state.event
        })
        recv.capture(limit=None, timeout=None, wakeup=True)


def run():
    args = parser.parse_args()

    setup_metrics(args.prefix)

    app = Celery()
    app.config_from_object(args.config)

    start_http_server(args.port, args.host)

    celery_monitor(app)


if __name__ == '__main__':
    run()

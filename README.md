# Celery Metrics

Listens to Celery events and exports them via the Prometheus client.

## Installation

Clone this repo, and then run setup.py.

```
% git clone https://github.com/kalibrr/celery-prometheus.git
% cd celery-prometheus
% python setup.py install
```

## Running

Run it like so:

```
% CELERY_METRICS_CONFIG_MODULE=<my_module> celery_metrics --host=<statsd hostname> --port=<statsd port> --prefix=<my prefix>
```

where `my_module` is the configuration module you've used to configure Celery.

Options:

* `host` (default `localhost`): Hostname of the StatsD-compatible broker to forward to
* `port` (default `8125`): Port of the StatsD-compatible broker to forward to
* `prefix` (default `celery`): Prefix to prepend to metrics sent to the broker

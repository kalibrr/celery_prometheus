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
% celery_metrics --config=<my_module> --host=<hostname to listen on> --port=<port to listen on> [--monitor-memory]
```

where `my_module` is the configuration module you've used to configure Celery.


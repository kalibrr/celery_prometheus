FROM python:3

RUN groupadd -r celery --gid=999 && useradd -r -g celery --uid=999 celery

WORKDIR /app

COPY --chown=celery requirements.txt .
RUN pip install -r requirements.txt --no-cache-dir

COPY --chown=celery . .
RUN pip install -e .

USER celery
ENTRYPOINT /usr/local/bin/celery_prometheus

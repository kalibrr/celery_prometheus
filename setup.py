# -*- encoding: utf-8 -*-

from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals
from __future__ import division

import os

from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(here, 'README.md')).read()

requires = [
    'celery>=3,<4',
    'prometheus_client>=0.0.21,<0.1',
    'six>=1.7.3',
    'psutil'
]

setup(name='celery_prometheus',
      version='0.2.1',
      description='Exports Celery metrics to Prometheus',
      long_description=README,
      classifiers=[
        "Programming Language :: Python",
      ],
      author='Tim Dumol',
      author_email='tim@kalibrr.com',
      url='',
      keywords='celery prometheus',
      packages=find_packages(),
      install_requires=requires,
      entry_points={
          'console_scripts': [
              'celery_prometheus = celery_prometheus.app:run'
          ]
      }
      )

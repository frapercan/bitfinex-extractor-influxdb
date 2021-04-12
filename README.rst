bitfinex-extractor-influxdb
===========================

.. image:: https://img.shields.io/pypi/v/bitfinex-extractor-influxdb.svg
    :target: https://pypi.python.org/pypi/bitfinex-extractor-influxdb
    :alt: Latest PyPI version

.. image:: https://travis-ci.com/frapercan/bitfinex-extractor-influxdb.svg?branch=main
    :target: https://travis-ci.com/frapercan/bitfinex-extractor-influxdb

.. image:: https://codecov.io/gh/frapercan/bitfinex-extractor-influxdb/branch/main/graph/badge.svg?token=Z5KZG308CW
    :target: https://codecov.io/gh/frapercan/bitfinex-extractor-influxdb

.. image:: https://readthedocs.org/projects/bitfinex-extractor-influxdb/badge/?version=latest
    :target: https://bitfinex-extractor-influxdb.readthedocs.io/en/latest/?badge=latest
    :alt: Documentation Status

Bitfinex candle extractor into InfluxDB.
You can replicate BFX values into a TSDB and let it running so it will synchronize the Exchange
into your local Database.

Usage
-----
DataSync().run() in a container with InfluxDB and MySQL running and configured through setting a ".env"
file into the root of the project. You have a sample as ".env.sample".



Installation
------------

https://portal.influxdata.com/downloads/

https://hub.docker.com/_/mysql

Requirements
^^^^^^^^^^^^

influxdb-client
numpy
PyMySQL
pandas
python-dotenv
setuptools
pendulum
requests

Compatibility
-------------
This is just a Python program that can run in any system.
It was developed using Ubuntu.

Licence
-------

Authors
-------

`bitfinex-extractor-influxdb` was written by `frapercan <frapercan1@alum.us.es>`_.

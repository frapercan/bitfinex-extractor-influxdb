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

.. image:: https://github.com/frapercan/bitfinex-extractor-influxdb/blob/develop/graphics/screenshot.png
    :target: https://github.com/frapercan/bitfinex-extractor-influxdb/blob/develop/graphics/screenshot.png
    :alt: InfluxDB Interface



Usage
-----

Set Up MySQL into your computer.
Create two tables:

    * pair: Add the desired timeseries to this table as rows (Choose from symbols):


    * timeframe: ['1m', '5m', '15m', '30m', '1h', '3h', '6h', '12h', '1D', '7D', '14D', '1M']

Those are the different time interval we are interested for each pair.


Set Up InfluxDB into your computer:

    * Add a bucket

Exchange Symbols can be found here: https://api-pub.bitfinex.com/v2/conf/pub:list:pair:exchange
Credentials and other settings are configured through a .env file in the root of the project.
There is a template as .env.sample

To start the extraction, execute DataSync().run()

It will start the process, fed the database and synchronize with new values.



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

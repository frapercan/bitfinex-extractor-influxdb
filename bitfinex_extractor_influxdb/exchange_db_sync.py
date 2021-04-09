import json
import sys
import time
import os
from dotenv import load_dotenv

import pendulum
import pymysql
import logging
import requests
from influxdb_client import InfluxDBClient, Point, WritePrecision
import datetime
from datetime import timezone

load_dotenv()

fmt = '[%(asctime)-15s] [%(levelname)s] %(name)s: %(message)s'
logging.basicConfig(format=fmt, level=logging.INFO, stream=sys.stdout)

HTTP_API_URL = 'https://api-pub.bitfinex.com/v2/'

ERROR_CODE_SUBSCRIPTION_FAILED = 10300
ERROR_CODE_RATE_LIMIT = 11010
INFO_CODE_RECONNECT = 20051
ERROR_CODE_START_MAINTENANCE = 20006

class DataSync:
    """This is a class representation of an exchange scrapper
    that looks for configurations in a MYSQL server, extracts
    from Bitfinex Exchange candlesticks for the chosen pairs
    and time interval. It will retrieve from the beginning of
    the timeserie to nowadays and dump that information into
    INFLUXDB (Time Series Database).



    :param mysql_cursor: :class:`pymysql.client.cursor` cursor
        object for reading and writing from MYSQL.
    :param pairs: A list containing all pairs configuration.

        They must exist as rows in the pair table in MYSQL.

        You can check the available symbols for pairs here:

         https://api-pub.bitfinex.com/v2/conf/pub:list:pair:exchange
    :type pairs: list
    :param timeframes: A list containing all timeframes configuration, run method will scrape one time series
        per each timeframe and pair.

        They must exist as rows in the timeframe table in MYSQL.

        Avaliable timeframes values: '1m', '5m', '15m', '30m', '1h', '3h', '6h', '12h', '1D', '7D', '14D', '1M'
    :type timeframes: list
    :param bucket: InfluxDB Bucket name.

            Configured using the environemnt variable "INFLUX_BUCKET"
    :type bucket: str
    :param org: InfluxDB organization name.

            Configured using the environemnt variable "INFLUX_ORG"
    :type org: str
    :param influx_client: :class:`InfluxDBClient` InfluxDB API client.

            Configured using the environemnt variables "INFLUX_URL" and "INFLUX_TOKEN"
    :type influx_client: InfluxDBClient
    :param timeseries_start: starting date for the timeseries to scrape.

            Configured using the environemnt variable "STARTING_YEAR"
    :type request_delay: int
    :param logger: :class:`Logger` log handler.
    :type logger: Logger
    """

    def __init__(self):
        # Load Environment variables
        load_dotenv()
        # Establish connection with MYSQL and generate an interface for querying and writing.
        self._mysql_cursor = pymysql.connect(**{'host': os.getenv("MYSQL_HOST"),
                                                'user': os.getenv("MYSQL_USER"),
                                                'password': os.getenv("MYSQL_PASSWORD"),
                                                'database': os.getenv("MYSQL_DATABASE"),
                                                'cursorclass': pymysql.cursors.DictCursor}).cursor()

        # Influxdb parameters loaded from MYSQL
        self._bucket = os.getenv("INFLUX_BUCKET")
        self._org = os.getenv("INFLUX_ORG")

        # Establish connection with INFLUXDB.
        self._influx_client = InfluxDBClient(url=os.getenv("INFLUX_URL"), token=os.getenv("INFLUX_TOKEN"))

        # Functional configuration through MYSQL interaction and environment variables.
        self._pairs = self.query_pairs()
        self._timeframes = self.query_timeframes()
        self._timeseries_start = datetime.datetime(int(os.getenv("STARTING_YEAR")), 1, 1, tzinfo=timezone.utc)
        self._request_delay = int(os.getenv("REQUEST_DELAY"))

        self._logger = logging.getLogger(self.__class__.__name__)


    @property
    def mysql_cursor(self):
        return self._mysql_cursor

    @property
    def pairs(self):
        return self._pairs

    @property
    def timeframes(self):
        return self._timeframes


    @property
    def bucket(self):
        return self._bucket

    @property
    def org(self):
        return self._org

    @property
    def influx_client(self):
        return self._influx_client

    @property
    def timeseries_start(self):
        return self._timeseries_start

    @property
    def request_delay(self):
        return self._request_delay

    @property
    def logger(self):
        return self._logger

    def query_pairs(self):
        """Query into MySQL's pair table and return the values.

        :return: A list of pairs
        :rtype: :class:`list(str)`
        """
        self.mysql_cursor.execute("SELECT * FROM pair;")
        return [pair['name'] for pair in self.mysql_cursor.fetchall()]

    def query_timeframes(self):
        """Query into MySQL's timeframe table and return the values.

        :return: A list of timeframes
        :rtype: :class:`list(str)`
        """
        self.mysql_cursor.execute("SELECT * FROM timeframe;")
        return [pair['interval'] for pair in self.mysql_cursor.fetchall()]

    def run(self):
        """Extract time series from Bitfinex Exchange and store them into InfluxDB .
        """
        for pair in self.pairs:
            for timeframe in self.timeframes:
                self._extract_series(pair, timeframe)

    def _extract_series(self, pair, timeframe):
        last_sample_timestamp_ns = self._get_last_sample_timestamp(pair, timeframe) * 1000
        while 1:
            url = url_generator(pair, timeframe, last_sample_timestamp_ns)
            json_response = requests.get(url)
            response = json.loads(json_response.text)

            if not self._check_bitfinex_connection(response):
                continue

            last_response_timestamp_ns = int(response[-1][0])
            if compare_timestamps(last_sample_timestamp_ns, last_response_timestamp_ns):
                self.logger.info('Correctly sync %s - %s', pair, timeframe)
                break
            try:
                self.influx_client.write_api().write(record=serialize_points(pair, timeframe, response), org=self.org,
                                                     bucket=self.bucket)
            except Exception as e:
                self.logger.warning('Couldnt write into INFLUXDB: %s', e)
                continue

            last_sample_timestamp_ns = last_response_timestamp_ns

    def _get_last_sample_timestamp(self, pair, timeframe):
        last_ts_query = f'from(bucket: "{self.bucket}") \
                |> range(start: -9999d) \
                |> filter(fn: (r) => r["_measurement"] == "{pair}") \
                |> filter(fn: (r) => r["timeframe"] == "{timeframe}") \
                |> filter(fn: (r) => r["_field"] == "open") \
                |> last(column: "_time") \
                |> yield(name: "last")'
        try:
            last_sample_date = self.influx_client.query_api().query_data_frame(last_ts_query, org=self.org)['_time'][0]

        except KeyError:
            last_sample_date = self.timeseries_start
        return int(last_sample_date.timestamp())

    def _check_bitfinex_connection(self, response):
        if 'error' in response:
            # Check rate limit
            if response[1] == ERROR_CODE_RATE_LIMIT:
                self.logger.info('Error: reached the limit number of requests. Wait 120 seconds...')
                time.sleep(20)

            # Check platform status
            if response[1] == ERROR_CODE_START_MAINTENANCE:
                self.logger.info('Error: platform is in maintenance. Forced to stop all requests.')
                time.sleep(1)
            return False
        return True


def url_generator(pair, timeframe, last_sample_timestamp_ns):
    print(HTTP_API_URL + f'candles/trade:{timeframe}:{pair}' \
                         f'/hist?limit=1000&start={last_sample_timestamp_ns}&sort=1')
    return HTTP_API_URL + f'candles/trade:{timeframe}:{pair}' \
                          f'/hist?limit=1000&start={last_sample_timestamp_ns}&sort=1'


def serialize_points(pair, timeframe, response):
    points = []
    for tick in list(response):
        open_price = float(tick[1])
        low_price = float(tick[2])
        high_price = float(tick[3])
        close_price = float(tick[4])
        volume = float(tick[5])
        timestamp = pendulum.from_timestamp(int(tick[0]) // 1000).in_tz('UTC').to_atom_string()
        point = Point(pair).field('close', close_price) \
            .tag('timeframe', timeframe) \
            .field('open', open_price) \
            .field('high', high_price) \
            .field('low', low_price) \
            .field('volume', volume) \
            .time(timestamp, WritePrecision.NS)
        points.append(point)
    return points


def compare_timestamps(last_sample_timestamp_ns, last_response_timestamp_ns):
    return last_sample_timestamp_ns == last_response_timestamp_ns


if __name__ == "__main__":
    DataSync().run()

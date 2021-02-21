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
    def __init__(self):
        self.mysql_cursor = pymysql.connect(host=os.getenv("HOST"),
                                            user=os.getenv("DB_USER"),
                                            password=os.getenv("PASSWORD"),
                                            database=os.getenv("DATABASE"),
                                            cursorclass=pymysql.cursors.DictCursor).cursor()

        self.pairs = self.query_pairs()
        self.timeframes = self.query_timeframes()
        self.logger = logging.getLogger(self.__class__.__name__)

        self.bucket = os.getenv("BUCKET")
        self.org = os.getenv("ORG")
        self.client = InfluxDBClient(url=os.getenv("URL"), token=os.getenv("TOKEN"))

        self.timeseries_start = datetime.datetime(int(os.getenv("STARTING_YEAR")), 1, 1, tzinfo=timezone.utc)
        self.request_delay = int(os.getenv("REQUEST_DELAY"))

    def query_pairs(self):
        self.mysql_cursor.execute("SELECT * FROM pair;")
        return [pair['name'] for pair in self.mysql_cursor.fetchall()]

    def query_timeframes(self):
        self.mysql_cursor.execute("SELECT * FROM timeframe;")
        return [pair['interval'] for pair in self.mysql_cursor.fetchall()]

    def run(self):
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
            print('pre')
            if compare_timestamps(last_sample_timestamp_ns, last_response_timestamp_ns):
                self.logger.info('Correctly sync %s - %s', pair, timeframe)
                print('dentro')
                break
            try:
                self.client.write_api().write(record=serialize_points(pair, timeframe, response), org=self.org,
                                              bucket=self.bucket)
            except:
                break

            last_sample_timestamp_ns = last_response_timestamp_ns
            time.sleep(1)

    def _get_last_sample_timestamp(self, pair, timeframe):
        last_ts_query = f'from(bucket: "{self.bucket}") \
            |> range(start: -9999d) \
            |> filter(fn: (r) => r["_measurement"] == "{pair}") \
            |> filter(fn: (r) => r["timeframe"] == "{timeframe}") \
            |> filter(fn: (r) => r["_field"] == "open") \
            |> last(column: "_time") \
            |> yield(name: "last")'
        try:
            last_sample_date = self.client.query_api().query_data_frame(last_ts_query, org=self.org)['_time'][0]
        except KeyError:
            last_sample_date = self.timeseries_start
        return int(last_sample_date.timestamp())

    def _check_bitfinex_connection(self, response):
        if 'error' in response:
            # Check rate limit
            if response[1] == ERROR_CODE_RATE_LIMIT:
                self.logger.info('Error: reached the limit number of requests. Wait 120 seconds...')
                time.sleep(120)

            # Check platform status
            if response[1] == ERROR_CODE_START_MAINTENANCE:
                self.logger.info('Error: platform is in maintenance. Forced to stop all requests.')
                time.sleep(1)
            return False
        return True


def url_generator(pair, timeframe, last_sample_timestamp_ns):
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

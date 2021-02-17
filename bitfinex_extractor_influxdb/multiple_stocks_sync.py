import json
import sys
import time

import pendulum
import pymysql
import logging
import requests
from influxdb_client import InfluxDBClient, Point, WritePrecision
import datetime
from datetime import timezone

from error_codes import ERROR_CODE_RATE_LIMIT, ERROR_CODE_START_MAINTENANCE

fmt = '[%(asctime)-15s] [%(levelname)s] %(name)s: %(message)s'
logging.basicConfig(format=fmt, level=logging.INFO, stream=sys.stdout)

http_api_url = 'https://api-pub.bitfinex.com/v2/'
request_delay = 1
mysql_cursor = pymysql.connect(host='localhost',
                               user='root',
                               password='root',
                               database='Database',
                               cursorclass=pymysql.cursors.DictCursor).cursor()

# You can generate a Token from the "Tokens Tab" in the UI
token = "Q3C0ka7m_kxAtzP2JZ3L3j7d4nElJ2CQHr5wBJuk_-zgMP2f2MO4agqQsPzGMvoyapfBCdS3MEkHZ1HVKKsCmw=="
org = "xaxi"
bucket = "Crypto"

timeseries_start = datetime.datetime(2019, 1, 1, tzinfo=timezone.utc)

client = InfluxDBClient(url="http://localhost:8086", token=token)


class DataSync:
    def __init__(self):
        self.pairs = self.query_pairs()
        self.timeframes = self.query_timeframes()
        self.logger = logging.getLogger(self.__class__.__name__)

    def query_pairs(self):
        mysql_cursor.execute("SELECT * FROM pair;")
        return [pair['name'] for pair in mysql_cursor.fetchall()]

    def query_timeframes(self):
        mysql_cursor.execute("SELECT * FROM timeframe;")
        return [pair['interval'] for pair in mysql_cursor.fetchall()]

    def run(self):
        for pair in self.pairs:
            for timeframe in self.timeframes:
                self.extract_serie(pair, timeframe)

    def extract_serie(self, pair, timeframe):
        last_ts_query = f'from(bucket: "{bucket}") \
            |> range(start: -9999d) \
            |> filter(fn: (r) => r["_measurement"] == "{pair}") \
            |> filter(fn: (r) => r["timeframe"] == "{timeframe}") \
            |> filter(fn: (r) => r["_field"] == "open") \
            |> last(column: "_time") \
            |> yield(name: "last")'
        try:
            last_sample_date = client.query_api().query_data_frame(last_ts_query, org=org)['_time'][0]
        except KeyError:
            last_sample_date = timeseries_start
        last_sample_timestamp = int(last_sample_date.timestamp())
        while 1:
            url = url_generator(pair, timeframe, last_sample_timestamp)
            json_response = requests.get(url)
            response = json.loads(json_response.text)
            time.sleep(request_delay)
            if 'error' in response:
                # Check rate limit
                if response[1] == ERROR_CODE_RATE_LIMIT:
                    print('Error: reached the limit number of requests. Wait 120 seconds...')
                    time.sleep(120)
                    continue
                # Check platform status
                if response[1] == ERROR_CODE_START_MAINTENANCE:
                    print('Error: platform is in maintenance. Forced to stop all requests.')
                    time.sleep(1)
                    continue
            else:
                last_response_timestamp = int(response[-1][0]) // 1000  # ns to ms
                if last_sample_timestamp == last_response_timestamp:
                    self.logger.info(
                        'Correctly sync {pair} - {timeframe} - {date}', pair=pair, timeframe=timeframe,
                        date=pendulum.from_timestamp(last_response_timestamp))
                    break
                last_sample_timestamp = last_response_timestamp
                client.write_api().write(record=serialize_points(pair, timeframe, response), org=org, bucket=bucket)


def url_generator(pair, timeframe, last_sample_timestamp):
    return http_api_url + f'candles/trade:{timeframe}:{pair}' \
                          f'/hist?limit=1000&start={last_sample_timestamp * 1000}&sort=1'


def serialize_points(pair, timeframe, response):
    print(response)
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
    # Return True is operation is successful
    return points


DataSync().run()

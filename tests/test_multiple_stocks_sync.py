import os

from mock import patch, MagicMock, Mock
from bitfinex_extractor_influxdb import multiple_stocks_sync

mock_pairs = ['tBTCUSD', 'tIOTUSD', 'tLTCBTC']
mock_timeframes = ['1m', '15m', '1h']

mock_fetch_pairs = [{'id': 3, 'name': 'tBTCUSD'}, {'id': 1, 'name': 'tIOTUSD'}, {'id': 2, 'name': 'tLTCBTC'}]
mock_fetch_timeframes = [{'id': 3, 'interval': '1m'}, {'id': 1, 'interval': '15m'}, {'id': 2, 'interval': '1h'}]

pair_test = mock_pairs[0]
timeframe_test = mock_timeframes[0]

environment_variables = {
    "HOST": "MYSQL_HOST",
    "USER": "MYSQL_USER",
    "PASSWORD": "MYSQL_PASSWORD",
    "DATABASE": "MYSQL_DATABASE",
    "URL": "INFLUXDB_HOST",
    "TOKEN": "INFLUXDB_TOKEN",
    "ORG": "INFLUXDB_ORG",
    "BUCKET": "INFLUXDB_BUCKET",
    "STARTING_YEAR": "1970",
    "REQUEST_DELAY": "1",
}


@patch("bitfinex_extractor_influxdb.multiple_stocks_sync.DataSync.query_timeframes")
@patch("bitfinex_extractor_influxdb.multiple_stocks_sync.DataSync.query_pairs")
@patch("pymysql.connect")
@patch.dict(os.environ, environment_variables)
def test_initialize(mock_pymsql, mock_query_pairs, mock_query_timeframes):
    mock_query_pairs.return_value = mock_pairs
    mock_query_timeframes.return_value = mock_timeframes

    sync = multiple_stocks_sync.DataSync()
    assert sync.pairs == mock_pairs
    assert sync.timeframes == mock_timeframes
    assert sync.bucket == environment_variables['BUCKET']
    assert sync.org == environment_variables['ORG']
    assert sync.request_delay == int(environment_variables['REQUEST_DELAY'])
    return sync


def test_query_pairs():
    sync = test_initialize()
    sync.mysql_cursor.fetchall.return_value = mock_fetch_pairs
    assert sync.query_pairs() == mock_pairs


def test_query_timeframes():
    sync = test_initialize()
    sync.mysql_cursor.fetchall.return_value = mock_fetch_timeframes
    assert sync.query_timeframes() == mock_timeframes


@patch("bitfinex_extractor_influxdb.multiple_stocks_sync.DataSync.extract_series")
def test_run(mock_extract_series):
    sync = test_initialize()
    sync.run()
    assert mock_extract_series.call_count == len(mock_pairs) * len(mock_timeframes)


# def test_extract_series():
#     sync = test_initialize()
#     sync.extract_series(pair_test,timeframe_test)

# def test_extract_series():
#     sync = test_initialize()
#     sync.run()

test_run()

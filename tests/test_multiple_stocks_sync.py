import os

from mock import patch, MagicMock, Mock
from bitfinex_extractor_influxdb import multiple_stocks_sync
import pickle

mock_pairs = ['tBTCUSD', 'tIOTUSD', 'tLTCBTC']
mock_timeframes = ['1m', '15m', '1h']

mock_fetch_pairs = [{'id': 3, 'name': 'tBTCUSD'}, {'id': 1, 'name': 'tIOTUSD'}, {'id': 2, 'name': 'tLTCBTC'}]
mock_fetch_timeframes = [{'id': 3, 'interval': '1m'}, {'id': 1, 'interval': '15m'}, {'id': 2, 'interval': '1h'}]

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


def _mock_response(
        status=200,
        content="CONTENT",
        json_data=None,
        raise_for_status=None):
    """
    since we typically test a bunch of different
    requests calls for a service, we are going to do
    a lot of mock responses, so its usually a good idea
    to have a helper function that builds these things
    """
    mock_resp = Mock()
    # mock raise_for_status call w/optional error
    mock_resp.raise_for_status = Mock()
    if raise_for_status:
        mock_resp.raise_for_status.side_effect = raise_for_status
    # set status code and content
    mock_resp.status_code = status
    mock_resp.content = content
    # add json data if provided
    if json_data:
        mock_resp.json = Mock(
            return_value=json_data
        )
    return mock_resp


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


@patch("bitfinex_extractor_influxdb.multiple_stocks_sync.DataSync._extract_series")
def test_run(mock_extract_series):
    sync = test_initialize()
    sync.run()
    assert mock_extract_series.call_count == len(mock_pairs) * len(mock_timeframes)


# @patch('requests.get')
# @patch('influxdb_client.InfluxDBClient.query_api')
# def test_extract_series(mock_query_api,mock_get):
#     mock_resp = _mock_response(content="ELEPHANTS")
#     mock_get.return_value = {'hola'}
#     mock_query_api.query_dataframe.return_value = 3
#     sync = test_initialize()
#     sync._extract_series(pair_test,timeframe_test)

# def test_extract_series():
#     sync = test_initialize()
#     sync.run()

pair_test = mock_pairs[0]
timeframe_test = mock_timeframes[0]
mock_url_generated = "https://api-pub.bitfinex.com/v2/candles/trade:12h:tBTCUSD/hist?limit=1000&start=1613908800000" \
                     "&sort=1 "
mock_last_sample_timestamp = 1613908800

#Simulating two correct iterations, the second one will be up to date.


@patch('influxdb_client.client.write_api.WriteApi.write',
       MagicMock(return_value=False))
@patch('bitfinex_extractor_influxdb.multiple_stocks_sync.compare_timestamps',
       MagicMock(side_effect=[False, True]))
@patch('bitfinex_extractor_influxdb.multiple_stocks_sync.DataSync._check_bitfinex_connection',
       MagicMock(return_value=True))
@patch('requests.get', MagicMock(return_value=pickle.load(open("./tests/json_response.p", "rb"))))
@patch('bitfinex_extractor_influxdb.multiple_stocks_sync.url_generator',
       MagicMock(return_value=mock_url_generated))
@patch('bitfinex_extractor_influxdb.multiple_stocks_sync.DataSync._get_last_sample_timestamp',
       MagicMock(return_value=mock_last_sample_timestamp))
def test_extract_series():
    sync = test_initialize()
    sync._extract_series(pair_test, timeframe_test)
    assert sync._get_last_sample_timestamp.call_count == 1
    assert multiple_stocks_sync.url_generator.call_count == 2
    assert sync._check_bitfinex_connection.call_count == 2
    assert multiple_stocks_sync.compare_timestamps.call_count == 2
    assert sync.client.write_api().write.call_count == 1



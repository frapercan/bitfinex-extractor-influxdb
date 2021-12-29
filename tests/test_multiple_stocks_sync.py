import os

from mock import patch, MagicMock, Mock
from bitfinex_extractor_influxdb import exchange_db_sync
import pickle

mock_pairs = ['tBTCUSD', 'tIOTUSD', 'tLTCBTC']
mock_timeframes = ['1m', '15m', '1h']

mock_fetch_pairs = [{'id': 3, 'name': 'tBTCUSD'}, {'id': 1, 'name': 'tIOTUSD'}, {'id': 2, 'name': 'tLTCBTC'}]
mock_fetch_timeframes = [{'id': 3, 'interval': '1m'}, {'id': 1, 'interval': '15m'}, {'id': 2, 'interval': '1h'}]

config = {
    "URL": "INFLUXDB_HOST",
    "TOKEN": "INFLUXDB_TOKEN",
    "ORGANIZATION": "INFLUXDB_ORG",
    "BUCKET": "INFLUXDB_BUCKET",
    "STARTING_YEAR": 1970,
    "REQUEST_DELAY": 1,
    "pairs": mock_pairs,
    "timeframes": mock_timeframes
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



def test_initialize():
    sync = exchange_db_sync.DataSync(config)
    assert sync.pairs == config['pairs']
    assert sync.timeframes == config['timeframes']
    assert sync.bucket == config['BUCKET']
    assert sync.org == config['ORGANIZATION']
    assert sync.request_delay == int(config['REQUEST_DELAY'])
    return sync


@patch("bitfinex_extractor_influxdb.exchange_db_sync.DataSync._extract_series")
def test_run(mock_extract_series):
    sync = test_initialize()
    sync.run()
    assert mock_extract_series.call_count == len(sync.pairs) * len(sync.timeframes)

@patch('requests.get')
@patch('influxdb_client.InfluxDBClient.query_api')
def test_extract_series(mock_query_api,mock_get):
    mock_resp = _mock_response(content="ELEPHANTS")
    mock_get.return_value = mock_resp
    mock_query_api.query_dataframe.return_value = 3
    sync = test_initialize()
    sync._extract_series(mock_pairs[0],mock_timeframes[0])

pair_test = mock_pairs[0]
timeframe_test = mock_timeframes[0]
mock_url_generated = "https://api-pub.bitfinex.com/v2/candles/trade:12h:tBTCUSD/hist?limit=1000&start=1613908800000" \
                     "&sort=1 "
mock_last_sample_timestamp = 1613908800


# Simulating two correct iterations, the second one will be up to date.


@patch('influxdb_client.client.write_api.WriteApi.write',
       MagicMock(return_value=False))
@patch('bitfinex_extractor_influxdb.exchange_db_sync.compare_timestamps',
       MagicMock(side_effect=[False, True]))
@patch('bitfinex_extractor_influxdb.exchange_db_sync.DataSync._check_bitfinex_connection',
       MagicMock(return_value=True))
@patch('requests.get', MagicMock(return_value=pickle.load(open("./tests/bitfinex_response_candle.p", "rb"))))
@patch('bitfinex_extractor_influxdb.exchange_db_sync.url_generator',
       MagicMock(return_value=mock_url_generated))
@patch('bitfinex_extractor_influxdb.exchange_db_sync.DataSync._get_last_sample_timestamp',
       MagicMock(return_value=mock_last_sample_timestamp))
def test_extract_series():
    sync = test_initialize()
    sync._extract_series(pair_test, timeframe_test)
    assert sync._get_last_sample_timestamp.call_count == 1
    assert exchange_db_sync.url_generator.call_count == 2
    assert sync._check_bitfinex_connection.call_count == 2
    assert exchange_db_sync.compare_timestamps.call_count == 2
    assert sync.influx_client.write_api().write.call_count == 1


# Bitfinex connection not stablished at first attempt, it will finish sync at second try.
@patch('influxdb_client.client.write_api.WriteApi.write',
       MagicMock(return_value=False))
@patch('bitfinex_extractor_influxdb.exchange_db_sync.compare_timestamps',
       MagicMock(return_value=True))
@patch('bitfinex_extractor_influxdb.exchange_db_sync.DataSync._check_bitfinex_connection',
       MagicMock(side_effect=[False, True]))
@patch('requests.get', MagicMock(return_value=pickle.load(open("./tests/bitfinex_response_candle.p", "rb"))))
@patch('bitfinex_extractor_influxdb.exchange_db_sync.url_generator',
       MagicMock(return_value=mock_url_generated))
@patch('bitfinex_extractor_influxdb.exchange_db_sync.DataSync._get_last_sample_timestamp',
       MagicMock(return_value=mock_last_sample_timestamp))
def test_no_connection():
    sync = test_initialize()
    sync._extract_series(pair_test, timeframe_test)
    assert sync._get_last_sample_timestamp.call_count == 1
    assert exchange_db_sync.url_generator.call_count == 2
    assert sync._check_bitfinex_connection.call_count == 2
    assert exchange_db_sync.compare_timestamps.call_count == 1
    assert sync.influx_client.write_api().write.call_count == 0


# Write call throws exception, first attempt. 'last_sample_timestamp_ns' shouldn't be updated that time.
@patch('influxdb_client.client.write_api.WriteApi.write',
       MagicMock(side_effect=[Exception('Test'), True]))
@patch('bitfinex_extractor_influxdb.exchange_db_sync.compare_timestamps',
       MagicMock(side_effect=[False, False, True, True]))
@patch('bitfinex_extractor_influxdb.exchange_db_sync.DataSync._check_bitfinex_connection',
       MagicMock(return_value=True))
@patch('requests.get', MagicMock(return_value=pickle.load(open("./tests/bitfinex_response_candle.p", "rb"))))
@patch('bitfinex_extractor_influxdb.exchange_db_sync.url_generator',
       MagicMock(return_value=mock_url_generated))
@patch('bitfinex_extractor_influxdb.exchange_db_sync.DataSync._get_last_sample_timestamp',
       MagicMock(return_value=mock_last_sample_timestamp))
def test_write_call_exception():
    sync = test_initialize()
    sync._extract_series(pair_test, timeframe_test)
    assert sync._get_last_sample_timestamp.call_count == 1
    assert exchange_db_sync.url_generator.call_count == 3
    assert sync._check_bitfinex_connection.call_count == 3
    assert exchange_db_sync.compare_timestamps.call_count == 3
    assert sync.influx_client.write_api().write.call_count == 2

    assert exchange_db_sync.url_generator.call_args_list[-2] != exchange_db_sync.url_generator.call_args_list[
        -1]


@patch('influxdb_client.client.query_api.QueryApi.query_data_frame',
       MagicMock(return_value=pickle.load(open("./tests/last_sample_query.p", "rb"))))
def test_get_last_sample_timestamp():
    sync = test_initialize()
    print(pickle.load(open("./tests/last_sample_query.p", "rb")))
    assert sync._get_last_sample_timestamp(pair_test, timeframe_test) == \
           pickle.load(open("./tests/last_sample_query.p", "rb"))['_time'][0].timestamp()


@patch('influxdb_client.client.query_api.QueryApi.query_data_frame',
       MagicMock(side_effect=KeyError('initialize timeserie')))
def test_get_last_sample_timestamp_key_exception():
    sync = test_initialize()
    assert sync._get_last_sample_timestamp(pair_test, timeframe_test) == sync.timeseries_start.timestamp()


def test_check_bitfinex_connection():
    sync = test_initialize()
    assert sync._check_bitfinex_connection(pickle.load(open("./tests/bitfinex_response_candle.p", "rb"))) == True


@patch('time.sleep', MagicMock(side_effect=None))
def test_check_bitfinex_connection_limit_rate():
    sync = test_initialize()
    assert sync._check_bitfinex_connection(pickle.load(open("./tests/bitfinex_response_limit_error.p", "rb"))) == False


@patch('time.sleep', MagicMock(side_effect=None))
def test_check_bitfinex_connection_maintenance():
    sync = test_initialize()
    assert sync._check_bitfinex_connection(['error', exchange_db_sync.ERROR_CODE_START_MAINTENANCE]) == False


url_generator_pair = 'tBTCUSD'
url_generator_timeframe = '14D'
url_generator_last_sample_timestamp_ns = '1616025600000'
mock_url_generator_expected = 'https://api-pub.bitfinex.com/v2/candles/trade:14D:tBTCUSD/hist?limit=1000&start=1616025600000&sort=1'


def test_url_generator():
    assert exchange_db_sync.url_generator(url_generator_pair, url_generator_timeframe,
                                          url_generator_last_sample_timestamp_ns) == mock_url_generator_expected


def test_compare_timestamp():
    assert exchange_db_sync.compare_timestamps(url_generator_last_sample_timestamp_ns,
                                               url_generator_last_sample_timestamp_ns) == True


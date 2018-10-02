import numpy as np
import pandas as pd
from datetime import timedelta, time
from pytz import timezone
from requests import get
from pandas.tseries.offsets import CustomBusinessDay
from zipline.utils.memoize import lazyval
from zipline.utils.calendars import (
    TradingCalendar,
    register_calendar,
    register_calendar_alias,
    deregister_calendar)
from zipline.utils.cli import maybe_show_progress
from zipline.data.bundles import register

BITMEX_REST_URL = 'https://testnet.bitmex.com/api/v1'


def _bitmex_rest(operation: str, params: dict = None) -> list:
    assert operation[0] == '/'
    if params is None:
        params = {}
    res = get(BITMEX_REST_URL+operation, params=params)
    assert res.ok
    res = res.json()
    assert type(res) is list
    return res


# def _get_metadata(sid_map: list):
#     metadata = pd.DataFrame(
#         np.empty(
#             len(sid_map),
#             dtype=[
#                 ('symbol', 'str'),
#                 ('root_symbol', 'str'),
#                 ('asset_name', 'str'),
#                 ('tick_size', 'float'),
#                 ('expiration_date', 'datetime64[ns]')
#             ]))
#     for sid, symbol in sid_map:
#         res = _bitmex_rest('/instrument', {'symbol': symbol})

#         assert len(res) == 1

#         res = res[0]
#         metadata.loc[sid, 'symbol'] = symbol
#         # metadata.loc[sid, 'root_symbol'] = symbol[:-3]
#         metadata.loc[sid, 'root_symbol'] = res['rootSymbol']
#         metadata.loc[sid, 'asset_name'] = symbol[:-3]
#         metadata.loc[sid, 'tick_size'] = res['tickSize']
#         metadata.loc[sid, 'expiration_date'] = None if symbol == 'XBTUSD' else pd.to_datetime(
#             res['expiry'])

#     metadata['exchange'] = 'bitmex'

#     return metadata


def _get_minute_bar(symbol: str, day_start: pd.Timestamp):
    day_end = day_start + timedelta(days=1, seconds=-1)
    res = []
    for _ in range(3):
        _res = _bitmex_rest(
            '/trade/bucketed',
            {
                'binSize': '1m',
                'count': 500,
                'symbol': symbol,
                'startTime': day_start.isoformat(),
                'endTime': day_end.isoformat(),
                'start': len(res)})
        assert len(_res) != 0
        res += _res
    assert len(res) == 24*60
    res = pd.DataFrame.from_dict(res)
    res.drop('symbol', axis=1, inplace=True)
    # I think this is a bug of pandas
    # res['timestamp'] = pd.to_datetime(res['timestamp'], utc=True)
    for i in range(res.shape[0]):
        res.loc[i, 'timestamp'] = pd.to_datetime(
            res.loc[i, 'timestamp'], utc=True)
    res.set_index('timestamp', inplace=True)
    assert res.shape[1] == 11
    return res


def _get_minute_bars(
        sid_map: list,
        start_session: pd.Timestamp,
        end_session: pd.Timestamp,
        cache):
    for sid, symbol in sid_map:
        for day in pd.date_range(start_session, end_session, freq='D', closed='left'):
            key = symbol+'-'+day.strftime("%Y-%m-%d")
            if key not in cache:
                cache[key] = _get_minute_bar(symbol, day)
            yield sid, cache[key]


def _get_metadata(sid: int, symbol: str, metadata: pd.DataFrame):
    res = _bitmex_rest('/instrument', {'symbol': symbol})

    assert len(res) == 1

    res = res[0]
    metadata.loc[sid, 'symbol'] = symbol
    metadata.loc[sid, 'root_symbol'] = res['rootSymbol']
    metadata.loc[sid, 'asset_name'] = res['rootSymbol']
    metadata.loc[sid, 'tick_size'] = res['tickSize']
    metadata.loc[sid, 'expiration_date'] = None if symbol == 'XBTUSD' else pd.to_datetime(
        res['expiry'])
    metadata['exchange'] = 'bitmex'


def _pricing_iter(metadata, symbols, show_progress, start_session, end_session, cache):
    sid = 0
    with maybe_show_progress(
            symbols,
            show_progress,
            label='BitMex pricing data: ') as it:

        for symbol in it:
            _get_metadata(sid, symbol, metadata)
            for day in pd.date_range(start_session, end_session, freq='D', closed='left'):
                key = symbol+'-'+day.strftime("%Y-%m-%d")
                if key not in cache:
                    cache[key] = _get_minute_bar(symbol, day)
                yield sid, cache[key]
            sid += 1


def bitmex(symbols: list):
    def ingest(
            environ,
            asset_db_writer,
            minute_bar_writer,
            daily_bar_writer,
            adjustment_writer,
            calendar,
            start_session,
            end_session,
            cache,
            show_progress,
            output_dir):

        metadata = pd.DataFrame(
            np.empty(
                len(symbols),
                dtype=[
                    ('symbol', 'str'),
                    ('root_symbol', 'str'),
                    ('asset_name', 'str'),
                    ('tick_size', 'float'),
                    ('expiration_date', 'datetime64[ns]')
                ]))

        minute_bar_writer.write(
            _pricing_iter(metadata, symbols, show_progress,
                          start_session, end_session, cache),
            show_progress=show_progress)

        asset_db_writer.write(futures=metadata)

        adjustment_writer.write()
    return ingest


class BitmexCalendar(TradingCalendar):
    @property
    def name(self):
        return "bitxex"

    @property
    def tz(self):
        return timezone('UTC')

    @property
    def open_time(self):
        return time(0, 0)

    @property
    def close_time(self):
        return time(23, 59)

    @lazyval
    def day(self):
        return CustomBusinessDay(
            weekmask='Mon Tue Wed Thu Fri Sat Sun'
        )

if __name__ == '__main__':
    register_calendar('bitxex', BitmexCalendar())

    register(
        'bitxex',
        bitmex(['XBTUSD']),
        calendar_name='bitxex',
        start_session=pd.Timestamp('2018-05-20', tz='utc'),
        end_session=pd.Timestamp('2018-06-10', tz='utc'),
        minutes_per_day=24*60)

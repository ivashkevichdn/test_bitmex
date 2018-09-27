import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from logbook import Logger

from catalyst import run_algorithm
from catalyst.api import (record, symbol, sid, order_target_percent,)
from catalyst.exchange.utils.stats_utils import extract_transactions

import datareaderbitmex as drbitmex

NAMESPACE = 'moving_average'
log = Logger(NAMESPACE)

def initialize(context):
    # Выбираем интесуемый акцив
    context.asset = symbol('btc_usd')

    # context.asset = sid(98871)
    context.i = 0

def handle_data(context, data):
    # Определить окна для скользящих средних
    short_window = 40
    long_window = 100

    # Пропустить первые 100 элементов, чтобы получить полное окно
    context.i += 1
    if context.i < long_window:
        return

   # Вычислим средние скользящие
    short_mavg = data.history(
        context.asset,
        'close',
        bar_count=short_window,
        frequency="1T",
    ).mean()

    long_mavg = data.history(
        context.asset,
        'close',
        bar_count=long_window,
        frequency="1T",
    ).mean()

    #Мы проверяем, какова наша позиция в нашем портфеле и соответственно
    pos_amount = context.portfolio.positions[context.asset].amount

    # Логика стратегии
    if short_mavg > long_mavg and pos_amount == 0:
        # Покупаеи акцивы в размере 10
        order_target_percent(context.asset, 10)
    elif short_mavg < long_mavg and pos_amount > 0:
        order_target_percent(context.asset, 0)

    # Сохранить значения для последующей визуализации
    record(
        XBTUSD=data.current(context.asset, 'close'),
        cash=context.portfolio.cash,
        short_mavg=short_mavg,
        long_mavg=long_mavg
    )


# Код взят из примера пока не расматривался.
def analyze(context=None, results=None):
    import matplotlib.pyplot as plt
    import logbook
    logbook.StderrHandler().push_application()
    log = logbook.Logger('Algorithm')

    results.to_csv('XB.csv')

    fig = plt.figure()
    ax1 = fig.add_subplot(211)
    results.portfolio_value.plot(ax=ax1)
    ax1.set_ylabel('Portfolio value (USD)')

    ax2 = fig.add_subplot(212)
    ax2.set_ylabel('Price (USD)')

    # If data has been record()ed, then plot it.
    # Otherwise, log the fact that no data has been recorded.
    if ('XBTUSD' in results and 'short_mavg' in results and
            'long_mavg' in results):
        results['XBTUSD'].plot(ax=ax2)
        results[['short_mavg', 'long_mavg']].plot(ax=ax2)

        trans = results.ix[[t != [] for t in results.transactions]]
        buys = trans.ix[[t[0]['amount'] > 0 for t in
                         trans.transactions]]
        sells = trans.ix[
            [t[0]['amount'] < 0 for t in trans.transactions]]
        ax2.plot(buys.index, results.short_mavg.ix[buys.index],
                 '^', markersize=10, color='m')
        ax2.plot(sells.index, results.short_mavg.ix[sells.index],
                 'v', markersize=10, color='k')
        plt.legend(loc=0)
    else:
        msg = 'XBTUSD, short_mavg & long_mavg data not captured using record().'
        ax2.annotate(msg, xy=(0.1, 0.5))
        log.info(msg)

    plt.show()

if __name__ == '__main__':

    # Период запрошаемых данных
    start_session = pd.to_datetime('2018-6-1', utc=True)
    end_session = pd.to_datetime('2018-6-4', utc=True)

    # # Путь нашего кэша-данных
    # path_cache = '/home/dmiv/.cachebitmex'

    # # Контракт
    # symbol = 'XBTUSD'

    # # Частота
    # data_frequency = '1m'

    # # Загрузим данные
    # dR = DataReaderBitmex(path_cash=path_cache,
    #                       symbol=symbol, data_frequency=data_frequency)
    # bars = dR.get_bars(start_session, end_session)
    # bars['symbol'] = 'btc_usd'

    run_algorithm(
        capital_base=100000,
        data_frequency='minute',
        initialize=initialize,
        handle_data=handle_data,
        analyze=analyze,
        exchange_name='bitmex',
        algo_namespace=NAMESPACE,
        live=False,
        default_extension=True,
        quote_currency='usd',
        start=start_session,
        end=end_session,
    )

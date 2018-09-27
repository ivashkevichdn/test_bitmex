#!/usr/bin/env python
#
# Copyright 2014 Quantopian, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Dual Moving Average Crossover algorithm.
This algorithm buys apple once its short moving average crosses
its long moving average (indicating upwards momentum) and sells
its shares once the averages cross again (indicating downwards
momentum).
"""
import pandas as pd
from zipline import run_algorithm
from zipline.api import order_target, record, symbol
from zipline.finance import commission, slippage

import datareaderbitmex as drbitmex

def initialize(context):
    context.asset = symbol('XBTUSD')
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
    short_mavg = data.history(context.asset, 'close', short_window, '1m').mean()
    long_mavg = data.history(context.asset, 'close', long_window, '1m').mean()

   #Мы проверяем, какова наша позиция в нашем портфеле и соответственно
    portfolio = context.portfolio

    # Логика стратегии
    if short_mavg > long_mavg:
        # Покупаеи акцивы в размере 10
        order_target(context.asset, 10)
    elif short_mavg < long_mavg:
        order_target(context.asset, 0)

    # Сохранить значения для последующей визуализации
    record(
        XBTUSD=data.current(context.asset, 'close'),
        cash=context.portfolio.cash,
        portfolio_value=context.portfolio.portfolio_value,
        positions=context.portfolio.positions,
        short_mavg=short_mavg,
        long_mavg=long_mavg
    )


# Note: this function can be removed if running
# this algorithm on quantopian.com
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

    start_session = pd.to_datetime('2018-6-1', utc=True)
    end_session = pd.to_datetime('2018-6-2', utc=True)

    run_algorithm(
        capital_base=100000,
        data_frequency='minute',
        initialize=initialize,
        handle_data=handle_data,
        bundle='bitmex',
        analyze=analyze,
        start=start_session,
        end=end_session,
    )

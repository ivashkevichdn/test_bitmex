"""


"""

import numpy as np
import pandas as pd
import logbook

from catalyst import run_algorithm
from catalyst.api import (record, symbol, sid, order_target_percent,)
from catalyst.exchange.utils.stats_utils import extract_transactions

import datareaderbitmex as drbitmex

NAMESPACE = 'moving_average_crossover'
log = logbook.Logger(NAMESPACE)


def initialize(context):
    # Выбираем интесуемый акцив
    context.asset = symbol('btc_usd')

    # Установим комиссию
    context.set_commission(taker=0.00075, maker=0.00025)
    context.i = 0

    # Установим объём покупаемых/продоваемый акцивов
    context.volume = 10

    context.base_price = None


def handle_data(context, data):
    # Определить окна для скользящих средних
    short_window = 40
    long_window = 100

    # Пропустить первые 100 элементов, чтобы получить полное окно
    context.i += 1
    if context.i < long_window:
        record(
            open=data.current(context.asset, 'open'),
            close=data.current(context.asset, 'close'),
            low=data.current(context.asset, 'low'),
            high=data.current(context.asset, 'high'),
            cash=context.portfolio.cash,
            price_change=0,
            portfolio_value=context.portfolio.portfolio_value,
            short_mavg=0,
            long_mavg=0
        )
        return

   # Вычислим средние скользящие
    short_mavg = data.history(
        context.asset,
        'close',
        bar_count=short_window,
        frequency="5T",
    ).mean()

    long_mavg = data.history(
        context.asset,
        'close',
        bar_count=long_window,
        frequency="5T",
    ).mean()

    # Мы проверяем, какова наша позиция в нашем портфеле и соответственно.
    pos_amount = context.portfolio.positions[context.asset].amount

    # Цена нашего акцива - в удобной форме.
    price = data.current(context.asset, 'close')

    # If base_price is not set, we use the current value. This is the
    # price at the first bar which we reference to calculate price_change.
    if context.base_price is None:
        context.base_price = price
    price_change = (price - context.base_price) / context.base_price

    # Since we are using limit orders, some orders may not execute immediately
    # we wait until all orders are executed before considering more trades.
    orders = context.blotter.open_orders
    if len(orders) > 0:
        return

    # Выход, если мы не можем торговать
    if not data.can_trade(context.asset):
        return

    # Логика стратегии
    if short_mavg > long_mavg and pos_amount == 0:
        # Покупаеи акцивы в размере 10
        order_target_percent(context.asset, context.volume)
    elif short_mavg < long_mavg and pos_amount > 0:
        order_target_percent(context.asset, 0)

    # Сохранить значения для последующей визуализации
    record(
        open=data.current(context.asset, 'open'),
        close=data.current(context.asset, 'close'),
        low=data.current(context.asset, 'low'),
        high=data.current(context.asset, 'high'),
        cash=context.portfolio.cash,
        price_change=price_change,
        portfolio_value=context.portfolio.portfolio_value,
        short_mavg=short_mavg,
        long_mavg=long_mavg
    )


# Код взят из примера пока не расматривался.
def analyze(context=None, results=None):
    from plotly.offline import plot
    import plotly.graph_objs as go

    # Сохраним результаты в csv формате
    results.to_csv('catalyst_portfolio.csv')

    trans = extract_transactions(results)
    buys  = trans[trans['amount'] > 0]
    sells = trans[trans['amount'] < 0]

    trans.to_csv('transaction.csv')
    buys.to_csv('buys.csv')
    sells.to_csv('sells.csv')
  # Построение графика курсов акцивов
    trace = go.Candlestick(
        x=results.index,
        open=results.open,
        high=results.high,
        low=results.low,
        close=results.close,
        increasing=dict(line=dict(width=1, color='#17BECF')),
        decreasing=dict(line=dict(width=1, color='#7F7F7F')),
        name='Курсы {}'.format(context.asset.asset_name),
        yaxis='y2'
    )

    # График короткой скользящей средней
    short_window = go.Scatter(
        x=results.short_mavg.index,
        y=results.short_mavg,
        line=dict(
            width=1,
            color='rgba(60, 190, 60, 1.0)'
        ),
        name='40-дневная SMA',
        yaxis='y2'
    )

    # График длинной скользящей средней
    long_window = go.Scatter(
        x=results.long_mavg.index,
        y=results.long_mavg,
        line=dict(
            width=1,
            color='rgba(180, 60, 170, 1.0)'
        ),
        name='100-дневная SMA',
        yaxis='y2'
    )

    # График покупок акцивов
    buy = go.Scatter(
        x=results.short_mavg[buys.index].index,
        y=results.short_mavg[buys.index],
        mode='markers',
        marker=dict(
            symbol="triangle-up",
            size=8,
            #         color='rgba(250, 128, 114, .9)',
             color='red',
             line=dict(width=1, color='rgba(0, 0, 0, 1.0)')
        ),
        name='Покупка акцивов',
        yaxis='y2'
    )

    # ПГрафик продажи акцивов
    sell = go.Scatter(
        x=results.short_mavg[sells.index].index,
        y=results.short_mavg[sells.index],
        mode='markers',
        marker=dict(
            symbol="triangle-down",
            size=8,
            color='blue',
            line=dict(width=1, color='black')
        ),
        name='Продажа акцивов',
        yaxis='y2'
    )

    # График общего капитала при торгах
    capital_total = go.Scatter(
        x=results.portfolio_value.index,
        y=results.portfolio_value,
        line=dict(
            width=1,
            color='blue'
        ),
        name='Значение Portfolio в $',
        yaxis='y1'
    )

    # График позиций - покупок на фоне общего капитала
    capital_buy = go.Scatter(
        x=results.portfolio_value[buys.index].index,
        y=results.portfolio_value[buys.index],
        mode='markers',
        marker=dict(
            symbol="triangle-up",
            size=8,
            color='red',
            line=dict(width=1, color='rgba(0, 0, 0, 1.0)')
        ),
        name='Покупка акцивов',
        yaxis='y1'
    )

    # График позиций - продаж на фоне общего капитала
    capital_sell = go.Scatter(
        x=results.portfolio_value[sells.index].index,
        y=results.portfolio_value[sells.index],
        mode='markers',
        marker=dict(
            symbol="triangle-down",
            size=8,
            color='black',
            line=dict(width=1, color='blue')
        ),
        name='Продажа акцивов',
        yaxis='y1'
    )

    # Создадим список наших постоенных графиков
    data = [trace, short_window, long_window, buy,
            sell, capital_total, capital_buy, capital_sell]

    # Настройки общего графика
    layout = go.Layout(
        paper_bgcolor='rgb(234, 233, 241)',
        plot_bgcolor='rgb(201, 187, 172)',
        title='График бэк-теста стратегии Moving Average Crossover на данных биржи BitMex',
        titlefont=dict(
            family='Arial, sans-serif',
            size=26,
            color='black'
        ),
        xaxis=dict(
            title='Дата',
            titlefont=dict(
                family='Arial, sans-serif',
                size=18,
                color='black'
            ),
            rangeslider=dict(
                visible=True
            ),
            range=['2018-06-01 00:00:00', '2018-06-05 00:00:00'],
            type='date'
        ),
        yaxis=dict(
            title='Капитал $',
            titlefont=dict(
                family='Arial, sans-serif',
                size=18,
                color='black'
            ),
            domain=[0, 0.47]
        ),
        yaxis2=dict(
            title='Kурсы {} $'.format(context.asset.asset_name),
            titlefont=dict(
                family='Arial, sans-serif',
                size=18,
                color='black'
            ),
            domain=[0.53, 1]
        )
    )

    # В итоге сторим наш общийграфик (создаем html файл)
    figure = go.Figure(data=data, layout=layout)
    plot(figure, filename='pandas_analyze')


if __name__ == '__main__':

    # Период запрошаемых данных
    start_session = pd.to_datetime('2018-6-1', utc=True)
    end_session = pd.to_datetime('2018-9-1', utc=True)

    run_algorithm(
        capital_base=100000,
        data_frequency='minute',
        initialize=initialize,
        handle_data=handle_data,
        analyze=analyze,
        exchange_name='gdax',
        algo_namespace=NAMESPACE,
        default_extension=True,
        quote_currency='usd',
        start=start_session,
        end=end_session,
    )

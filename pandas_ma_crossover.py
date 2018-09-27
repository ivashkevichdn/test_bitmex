import datetime
import numpy as np
import pandas as pd

import matplotlib.pyplot as plt

from plotly import tools
from plotly.offline import download_plotlyjs, init_notebook_mode, plot, iplot
import plotly.graph_objs as go

from metabacktest import Strategy, Portfolio


class MovingAverageCrossStrategy(Strategy):
    """    

    """

    def __init__(self, symbol: str, bars: pd.DataFrame, short_window: int = 40, long_window: int = 100):
        self.symbol = symbol
        self.bars = bars

        self.short_window = short_window
        self.long_window = long_window

        self.signals = pd.DataFrame(index=self.bars.index)
        self.signals['signal'] = 0.0

        # Создаём набор shor и long простых скользящих средних за соответствующие периоды
        self.signals['short_mavg'] = pd.rolling_mean(
            self.bars['close'], self.short_window, min_periods=1)
        self.signals['long_mavg'] = pd.rolling_mean(
            self.bars['close'], self.long_window,  min_periods=1)

        # Создайте 'signal' (инвестированный или не инвестированный), когда shor (короткая) простая скользящая средняя пересекает
        # long (длинную) простую скользящую средную.
        self.signals['signal'][self.short_window:] = np.where(self.signals['short_mavg'][self.short_window:]
                                                              > self.signals['long_mavg'][self.short_window:], 1.0, 0.0)

        # Принимайте разницу в сигналах, чтобы генерировать фактические торговые ордеры
        self.signals['positions'] = self.signals['signal'].diff()

    def get_signals(self):
        """
        Возвращает DataFrame символов, содержащих сигналы
        набор длинным, коротким или удерживать (1, -1 или 0).
        """
        return self.signals


class MarketOnClosePortfolio(Portfolio):
    """
    Наследует Portfolio для создания системы, которая покупает volume единиц конкретного актива,
    следуя сигналу по рыночной цене открытия бара.

    Кроме того, транзакционные издержки равны нулю, а средства для короткой продажи можно привлечь
    моментально без ограничений.

    Требования:
        symbol  - Акция, формирующая основу портфолио.
        bars    - Датафрейм баров для набора актива.
        signals - Датафрейм pandas сигналов (1, 0, -1) для каждого актива.
        volume  - Объёма покупаемых активов.
        capital - Объём средств на старте торговли.
    """

    def __init__(self, symbol: str, bars: pd.DataFrame, signals: pd.DataFrame, volume: int = 10, capital: float = 100000.0):
        self.symbol = symbol
        self.bars = bars
        self.signals = signals
        self.capital = float(capital)
        self.volume = volume

        self.portfolio = pd.DataFrame(index=self.bars.index)
        self.positions = pd.DataFrame(index=self.bars.index)

        self.positions['signal'] = self.signals['signal'] * self.volume
        self.positions['positions'] = self.signals['positions'] * self.volume

    def backtest(self):
        self.portfolio['holdings'] = self.positions['signal'] * \
            self.bars['close']
        self.portfolio['cash'] = self.capital - \
            (self.positions['positions'] * self.bars['close']).cumsum()

        self.portfolio['total'] = self.portfolio['cash'] + \
            self.portfolio['holdings']
        self.portfolio['returns'] = self.portfolio['total'].pct_change()
        self.portfolio.to_csv('portfolio.csv')

    def get_positions(self):
        return self.positions

    def get_portfolio(self):
        return self.portfolio

    def analyze(self):
        trace = go.Candlestick(
            x=self.bars.index,
            open=self.bars.open,
            high=self.bars.high,
            low=self.bars.low,
            close=self.bars.close,
            name='{} курсы'.format(self.symbol))

        short_window = go.Scatter(
            x=self.bars.index,
            y=self.signals.short_mavg,
            line=dict(
                width=2,
                color='rgba(60, 190, 60, 1.0)'
            ),
            name='40-дневная SMA')

        long_window = go.Scatter(
            x=self.bars.index,
            y=self.signals.long_mavg,
            line=dict(
                width=2,
                color='rgba(180, 60, 170, 1.0)'
            ),
            name='100-дневная SMA')

        buy = go.Scatter(
            x=self.bars.index[self.signals.positions == 1.0],
            y=self.signals.short_mavg[self.signals.positions == 1.0],
            mode='markers',
            marker=dict(
                symbol="triangle-up",
                size=8,
                color='rgba(250, 128, 114, .9)',
                line=dict(width=1, color='rgba(0, 0, 0, 1.0)')
            ),
            name='Покупка')

        sell = go.Scatter(
            x=self.bars.index[self.signals.positions == -1.0],
            y=self.signals.short_mavg[self.signals.positions == -1.0],
            mode='markers',
            marker=dict(
                symbol="triangle-down",
                size=8,
                color='rgba(25, 25, 112, .9)',
                line=dict(width=1, color='rgba(0, 0, 0, 1.0)')
            ),
            name='Продажа')

        data_1 = [trace, short_window, long_window, buy, sell]

        layout_1 = {
            'title': 'График {}'.format(self.symbol),
            'yaxis': {'title': '{} курсы'.format(self.symbol)},
            'xaxis': {'title': 'Дата'}
        }

        capital_total = go.Scatter(
            x=self.bars.index,
            y=self.portfolio.total,
            line=dict(
                width=2,
                color='rgba(0, 0, 0, 1.0)'
            ),
            name='Значение Portfolio в $')

        capital_buy = go.Scatter(
            x=self.bars.index[self.signals.positions == 1.0],
            y=self.portfolio.total[self.signals.positions == 1.0],
            mode='markers',
            marker=dict(
                symbol="triangle-up",
                size=8,
                color='rgba(250, 128, 114, .9)',
                line=dict(width=1, color='rgba(0, 0, 0, 1.0)')
            ),
            name='Покупка')

        capital_sell = go.Scatter(
            x=self.bars.index[self.signals.positions == -1.0],
            y=self.portfolio.total[self.signals.positions == -1.0],
            mode='markers',
            marker=dict(
                symbol="triangle-down",
                size=8,
                color='rgba(25, 25, 112, .9)',
                line=dict(width=1, color='rgba(0, 0, 0, 1.0)')
            ),
            name='Продажа')

        data_2 = [capital_total, capital_buy, capital_sell]

        layout_2 = {
            'title': 'График Portfolio',
            'yaxis': {'title': '{} курсы'.format(self.symbol)},
            'xaxis': {'title': 'Значение Portfolio в $'}
        }

        fig = tools.make_subplots(rows=2, cols=1, shared_xaxes=True)

        for it in data_1:
            fig.append_trace(it, 1, 1)

        for it in data_2:
            fig.append_trace(it, 2, 1)

        fig.layout.update(height=800, width=1400,
                             title='Бэк-тест торговой стратегии')
        

        # fig = dict(data=data, layout=layout)
        plot(fig, filename='{}_SMA'.format(self.symbol))


if __name__ == "__main__":
    import datareaderbitmex as drbitmex

    # Путь нашего кэша-данных
    path_cache = '/home/dmiv/.cachebitmex'

    # Контракт
    symbol = 'XBTUSD'

    # Период запрошаемых данных
    start_session = pd.to_datetime('2018-6-1', utc=True)
    end_session = pd.to_datetime('2018-6-4', utc=True)

    # Частота
    data_frequency = '1m'

    # Окна скользящих средних
    short_window = 40
    long_window = 100

    # Загрузим данные
    dR = drbitmex.DataReaderBitmex(path_cash=path_cache,
                          symbol=symbol, data_frequency=data_frequency)
    bars = dR.get_bars(start_session, end_session)
    # bars = pd.read_csv('bitmex_2018-6-1_2018-9-1.csv', sep=',')

    # Создайте экземпляр перекрестной стратегии скользящего среднего с
    # коротким скользящим средним окном в 40 дней и длинным окном в 100 дней
    mac = MovingAverageCrossStrategy(symbol=symbol,
                                     bars=bars,
                                     short_window=short_window,
                                     long_window=long_window
                                     )
    signals = mac.get_signals()

    # Создайте портфолио XBTUSD с первоначальным капиталом в размере $100000
    portfolio = MarketOnClosePortfolio(symbol, bars, signals, capital=100000.0)

    # Ну и сам backtest
    portfolio.backtest()

    # Визуализируем
    portfolio.analyze()

    returns = portfolio.get_portfolio()
    positions = portfolio.get_positions()

    # Сохраним результаты в csv формате
    signals.to_csv('signals_pandas.csv')
    returns.to_csv('portfolio_pandas.csv')
    positions.to_csv('positions_pandas.csv')

    print('++++++++++END!!!++++++++++')
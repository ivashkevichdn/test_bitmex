"""


"""

import datetime
import numpy as np
import pandas as pd

from plotly.offline import plot
import plotly.graph_objs as go

from metabacktest import Strategy, Portfolio


class MovingAverageCrossStrategy(Strategy):
    """    
    Объект описывающий логику торговой стратегии 
    Moving Average Crossover

    Требования:
        symbol - Симбол валютной пары
        bars  - Данные курса акцива
        short_window - Окно короткой средней скользящей
        long_window -  Окно длинной средней скоьзящей
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
        Возвращает объект DataFrame символов, содержащих сигналы
        набор длинным, коротким или удерживать (1, -1 или 0).
        """
        return self.signals

    def get_bars(self):
        """
        Возвращает объект DataFrame с рыночными данными.
        """
        return self.bars

    def get_symbol(self):
        """
        Возвращает объект DataFrame символ инструмента.
        """
        return self.symbol


class MarketOnClosePortfolio(Portfolio):
    """
    Наследует Portfolio для создания системы, которая покупает volume единиц конкретного актива,
    следуя сигналу по рыночной цене открытия бара.

    Кроме того, транзакционные издержки равны нулю, а средства для короткой продажи можно привлечь
    моментально без ограничений.

    Требования:
        strategy  - Объёкт Strategy описывающий логику торговой стратегии
        volume  - Объём покупаемых активов.
        capital - Объём средств на старте торговли.
    """

    def __init__(self, strategy: Strategy = None, volume: int = 10, capital: float = 1000000.0):
        self.symbol = strategy.get_symbol()
        self.bars = strategy.get_bars()
        self.signals = strategy.get_signals()
        self.capital = float(capital)
        self.volume = volume
        # Комиссия
        self.maker = 0.00025  # При покупки акцива
        self.taker = 0.00075  # При продаже акцива

        self.portfolio = pd.DataFrame(index=self.bars.index)

        self.portfolio['signal'] = self.signals['signal'] * self.volume
        self.portfolio['positions'] = self.signals['positions'] * self.volume

    def backtest(self):
        # Расчет средств на вкладах
        self.portfolio['holdings'] = self.portfolio['signal'] * \
            self.bars['close']

        # Расчет комиссий
        self.portfolio['comission_maker'] = self.maker * \
            np.where(self.portfolio['positions'] > 0, self.volume, 0.0) * self.bars['close']
        self.portfolio['comission_taker'] = self.taker * \
            np.where(self.portfolio['positions'] < 0, self.volume, 0.0) * self.bars['close']
        
        # Расчет комиссий (этот вариант аналогичен)
        #self.portfolio['comission_maker'] = self.maker * \
        #    self.portfolio['positions'].where(
        #        self.portfolio['positions'] > 0, 0).abs() * self.bars['close']
        #
        #self.portfolio['comission_taker'] = self.taker * \
        #    self.portfolio['positions'].where(
        #        self.portfolio['positions'] < 0, 0).abs() * self.bars['close']

        # Расчет "кошелька" с учетом комиссий
        self.portfolio['cash'] = self.capital - \
            self.portfolio['comission_maker'] - self.portfolio['comission_taker'] - \
            (self.portfolio['positions'] * self.bars['close']).cumsum()

        # Расчет общих средств (баланс)
        self.portfolio['total'] = self.portfolio['cash'] + \
            self.portfolio['holdings']
        self.portfolio['change'] = self.portfolio['total'].pct_change()

    def get_portfolio(self):
        return self.portfolio

    def analyze(self):

        # Построение графика курсов акцивов
        trace = go.Candlestick(
            x=self.bars.index,
            open=self.bars.open,
            high=self.bars.high,
            low=self.bars.low,
            close=self.bars.close,
            increasing=dict(line=dict(width=1, color='#17BECF')),
            decreasing=dict(line=dict(width=1, color='#7F7F7F')),
            name='Курсы {}'.format(self.symbol),
            yaxis='y2'
        )

        # График короткой скользящей средней
        short_window = go.Scatter(
            x=self.signals.short_mavg.index,
            y=self.signals.short_mavg,
            line=dict(
                width=1,
                color='rgba(60, 190, 60, 1.0)'
            ),
            name='40-дневная SMA',
            yaxis='y2'
        )

        # График длинной скользящей средней
        long_window = go.Scatter(
            x=self.signals.long_mavg.index,
            y=self.signals.long_mavg,
            line=dict(
                width=1,
                color='rgba(180, 60, 170, 1.0)'
            ),
            name='100-дневная SMA',
            yaxis='y2'
        )

        # График покупок акцивов
        buy = go.Scatter(
            x=self.signals.short_mavg[self.signals.positions == 1.0].index,
            y=self.signals.short_mavg[self.signals.positions == 1.0],
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
            x=self.signals.short_mavg[self.signals.positions == -1.0].index,
            y=self.signals.short_mavg[self.signals.positions == -1.0],
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
            x=self.portfolio.total.index,
            y=self.portfolio.total,
            line=dict(
                width=1,
                color='blue'
            ),
            name='Значение Portfolio в $',
            yaxis='y1'
        )

        # График позиций - покупок на фоне общего капитала
        capital_buy = go.Scatter(
            x=self.portfolio.total[self.signals.positions == 1.0].index,
            y=self.portfolio.total[self.signals.positions == 1.0],
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
            x=self.portfolio.total[self.signals.positions == -1.0].index,
            y=self.portfolio.total[self.signals.positions == -1.0],
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
                title='Kурсы {} $'.format(self.symbol),
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


if __name__ == "__main__":
    import datareaderbitmex as drbitmex

    # Путь нашего кэша-данных
    path_cache = './cachebitmex'

    # Контракт
    symbol = 'XBTUSD'

    # Начальный капитал
    capital = 100000.0

    # Период запрошаемых данных
    start_session = pd.to_datetime('2018-6-1', utc=True)
    end_session = pd.to_datetime('2018-9-1', utc=True)

    # Частота
    data_frequency = '5m'

    # Окна скользящих средних
    short_window = 40
    long_window = 100

    # Загрузим данные
    dR = drbitmex.DataReaderBitmex(path_cash=path_cache,
                                   symbol=symbol, data_frequency=data_frequency)
    bars = dR.get_bars(start_session, end_session)

    # Создайте экземпляр перекрестной стратегии скользящего среднего с
    # коротким скользящим средним окном и длинным окном.
    strategy = MovingAverageCrossStrategy(symbol=symbol,
                                          bars=bars,
                                          short_window=short_window,
                                          long_window=long_window
                                          )
    # Создайте портфолио XBTUSD с первоначальным капиталом
    context = MarketOnClosePortfolio(strategy=strategy, capital=capital)

    # Ну и сам backtest
    context.backtest()

    # Визуализируем
    context.analyze()

    # Сохраним результаты в csv формате
    context.get_portfolio().to_csv('portfolio_pandas.csv')

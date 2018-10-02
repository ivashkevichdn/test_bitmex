"""


"""
import datetime as dt
import bitmex as bm
import pandas as pd
import bravado
import time
import sys
from os import path, mkdir


class DataReaderBitmex:
    """
    Класс для получение данных с BitMex при этом он кэширует данные

    Требования:
        path_cash  - Путь к кэшу
        symbol  - Символ акцива в bitmex`е.
        data_frequency - Частота запрашиваемых курсов акцивов в bitmex.
        test - Работать с bitmex в тестовом режиме.
        api_key - Key зарегистрированного пользователя в bitmex.
        api_secret - Секретный код зарегистрированного пользователя в bitmex.
    """

    def __init__(self,
                 path_cash: str = './cachebitmex',
                 symbol: str = 'XBTUSD',
                 data_frequency: str = '1m',
                 test: bool = True,
                 api_key: str = None,
                 api_secret: str = None
                 ):

        self.client = bm.bitmex(
            test=test,
            api_key=api_key,
            api_secret=api_secret)

        self.set_path(path_cash)
        self.set_symbol(symbol)
        self.set_binSize(data_frequency)

    def set_path(self, path_cash: str):
        pt = path.abspath(path_cash)

        if not path.exists(pt):
            print('Ошибка: путь', path_cash, 'не найден или не существует!')
            sys.exit(1)

        self.path_cash = pt

    def get_path(self):
        return self.path_cash

    def set_symbol(self, symbol: str):
        pt = path.join(self.path_cash, symbol)

        if not path.exists(pt):
            mkdir(pt)

        self.symbol = symbol

    def get_symbol(self):
        return self.symbol

    def set_binSize(self, data_frequency: str):
        pt = path.join(self.path_cash, self.symbol, data_frequency)

        if not path.exists(pt):
            mkdir(pt)

        self.data_frequency = data_frequency

    def get_binSize(self):
        return self.data_frequency

    def check_cache(self, day: pd.Timestamp):
        """
        Метод проверки на существование за кешированных данных.
        day - Дата
        TODO: Не реализованно проверка на полноту данных в файле.
        """

        pt = path.join(
            self.path_cash,
            self.symbol,
            self.data_frequency,
            day.strftime("%Y-%m-%d")+'.csv')

        return path.exists(pt)

    def load_bar_day(self, day: pd.Timestamp):
        """
        Метод загрузки данных с сервера за сутки.
        day - Дата (тип: datetime.date() или datetime.datetime())
        """
        # TODO: Переделать надо, так как за последние сутки bitmex вылаживает не полностью
        #assert day.date() < (pd.Timestamp.today() - dt.timedelta(days=1))
        
        columns = ['timestamp','symbol', 'open', 'high', 'low', 'close', 'volume']
        
        loop_time = start_time = day
        end_time = start_time + dt.timedelta(days=1)
        start = 0

        while loop_time < end_time:
            try:
                [data, header] = self.client.Trade.Trade_getBucketed(
                    symbol=self.symbol,
                    binSize=self.data_frequency,
                    start=start,
                    startTime=start_time,
                    endTime=end_time,
                    count=500
                ).result()

            except bravado.exception.HTTPTooManyRequests as e:
                """
                Ошибка 429 - Превышен лимит по запросам.
                """
                print('Status Code:', e.status_code)
                print(e.message)

                retry_after = int(e.response.headers._store['retry-after'][1])

                # Произведем задержку на retry_after секунд.
                time.sleep(retry_after)

            except bravado.exception.HTTPServiceUnavailable as e:
                """
                Ошибка 503 - Сервер перегужен.
                """
                print('Status Code:', e.status_code)
                print(e.message)

                # Произведем задержку на 0,5 секунд.
                time.sleep(0.5)

            except bravado.exception.HTTPBadRequest as e:
                """
                TODO: Не до реализован!
                Ошибка 400.
                """
                print('Status Code:', e.status_code)
                print(e.message)

            except bravado.exception.HTTPUnauthorized as e:
                """
                TODO: Не до реализован!
                Ошибка 401.
                """
                print('Status Code:', e.status_code)
                print(e.message)

            except bravado.exception.HTTPForbidden as e:
                """
                TODO: Не до реализован!
                Ошибка 403.
                """
                print('Status Code:', e.status_code)
                print(e.message)

            except bravado.exception.HTTPNotFound as e:
                """
                TODO: Не до реализован!
                Ошибка 404 - Ресурс не найден.
                """
                print('Status Code:', e.status_code)
                print(e.message)

            else:
                assert len(data) != 0

                if not start:
                    df = pd.DataFrame(data=data, columns=columns)
                else:
                    df = df.append(pd.DataFrame(data=data, columns=columns), ignore_index=True)

                loop_time = df.timestamp[-1]

                start += len(data)

                # ++++++ Для отладки +++++++
                x_ratelimit_limit = int(
                    header.headers._store['x-ratelimit-limit'][1])
                x_ratelimit_remainind = int(
                    header.headers._store['x-ratelimit-remaining'][1])
                x_ratelimit_reset = int(
                    header.headers._store['x-ratelimit-reset'][1])

                print('DEBUG: Ограничение пакетов:', x_ratelimit_limit,
                      'Обратный счетчик пакетов:', x_ratelimit_remainind)
                # +++++++++++++++++++++++++++
                # Делаем задержку дабы лимитирующий счетчик не тикал на уменьшение.
                # Можно и 1,5 секунды, но сделаем на верняка - 2 сек.
                time.sleep(2)

        df.set_index('timestamp', inplace=True)
        df.index.name = 'last_traded'

        return df.loc[df.index < str(end_time)]

    def load_to_cache(self, start_time: pd.Timestamp, end_time: pd.Timestamp):
        """
        Метод загрузки данных в диапазоне (end_time - start_time) дней
        с сервера на файл формата CSV.
        start_time - Начальное время (тип: pd.Timestamp())
        end_time   - Конечное время (тип: pd.Timestamp())
        """
        # TODO: Переделать надо, так как за последние сутки bitmex вылаживает не полностью
        #assert end_time < (pd.Timestamp.today() - dt.timedelta(days=1))

        assert start_time < end_time

        if start_time.date() == end_time.date():
            if not self.check_cache(start_time):
                self.load_bar_day(start_time).to_csv(
                    path.join(
                        self.path_cash,
                        self.symbol,
                        self.data_frequency,
                        start_time.strftime("%Y-%m-%d") + '.csv'
                    )
                )
        else:
            for day in pd.date_range(start_time, end_time, freq='D', closed='left'):
                if not self.check_cache(day):
                    self.load_bar_day(day).to_csv(
                        path.join(
                            self.path_cash,
                            self.symbol,
                            self.data_frequency,
                            day.strftime("%Y-%m-%d") + '.csv'
                        )
                    )

    def load_from_cache(self, start_time: pd.Timestamp, end_time: pd.Timestamp):
        """
        Метод загрузки данных в диапазоне (end_time - start_time) дней
        с сервера на файл формата CSV.
        start_time - Начальная дата (тип: pd.Timestamp())
        end_time   - Конечная дата (тип: pd.Timestamp())
        """
        # TODO: Переделать надо, так как за последние сутки bitmex вылаживает не полностью
        #assert end_time.date() < (pd.Timestamp.today() - dt.timedelta(days=1))
        assert start_time < end_time

        self.load_to_cache(start_time, end_time)

        loop_time = start_time

        df = pd.read_csv(
            path.join(
                self.path_cash,
                self.symbol,
                self.data_frequency,
                loop_time.strftime("%Y-%m-%d") + '.csv'
            ),
            sep=','
        )

        loop_time += dt.timedelta(days=1)

        while loop_time < end_time:
            df = df.append(
                pd.read_csv(
                    path.join(
                        self.path_cash,
                        self.symbol,
                        self.data_frequency,
                        loop_time.strftime("%Y-%m-%d") + '.csv'
                    ),
                    sep=','
                ),
                ignore_index=True
            )
            loop_time += dt.timedelta(days=1)
        df.set_index('last_traded', inplace=True)
        return df

    def get_bars(self, start_time: pd.Timestamp, end_time: pd.Timestamp):
        """
        Метод получение данных формате DataFrame за период.
        start_time - Начальная дата и время (тип: pd.Timestamp())
        end_time   - Конечная дата и время (тип: pd.Timestamp())
        """
        # TODO: Переделать надо, так как за последние сутки bitmex вылаживает не полностью
        #assert end_time.date() < (pd.Timestamp.today() - dt.timedelta(days=1))
        assert start_time < end_time

        df = self.load_from_cache(start_time, end_time)

        # return df.loc[str(start_time) : str(end_time), :]
        return df.loc[df.index >= str(start_time)][df.index < str(end_time)]


if __name__ == "__main__":

    # Путь нашего локального кэша-данных
    path_cache = './cachebitmex'

    # Контракт
    symbol = 'XBTUSD'

    # Период запрошаемых данных
    start_session = pd.to_datetime('2018-6-1', utc=True)
    end_session = pd.to_datetime('2018-9-1', utc=True)

    # Частота
    data_frequency = '1m'

    # Загрузим данные
    dR = DataReaderBitmex(path_cash=path_cache,
                          symbol=symbol, data_frequency=data_frequency)
    df = dR.get_bars(start_session, end_session)
    df['symbol'] = 'btc_usd'

    bars = df.copy()
    bars.set_index('symbol', inplace=True)
    bars = bars.assign(last_traded=df.index)

    bars.to_csv('bitmex_out.csv')

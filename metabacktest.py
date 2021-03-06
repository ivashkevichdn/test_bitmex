
from abc import ABCMeta, abstractmethod

class Strategy(object):
    """
    Strategy — это абстрактный базовый класс, предоставляющий интерфейсы для наследованных торговых стратегий 
    Цель наличия отедльно объекта Strategy заключается в выводе списка сигналов, которые формируют временной ряд индексированных дата-фреймов pandas.
    В данной реализации поддерживается лишь работа с одним финансовым инструментов.
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def get_signals(self):
        """
        Необходимо вернуть DataFrame с символами, содержащий сигналы для открытия длинной 
        или короткой позиции, или удержания таковой  (1, -1 or 0).
        """
        raise NotImplementedError("Should implement get_signals()!")
    
    @abstractmethod
    def get_bars(self):
        """
        Возвращает объект DataFrame с рыночными данными.
        """
        raise NotImplementedError("Should implement get_bars()!")

    @abstractmethod
    def get_symbol(self):
        """
        Возвращает объект DataFrame символ инструмента.
        """
        raise NotImplementedError("Should implement get_symbol()!")

class Portfolio(object):
    """
    Абстрактный базовый класс представляет портфолио позиций (инструменты и доступные средства),
    определенное на основе набора сигналов от объекта Strategy.
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def backtest(self):
        """
        Обеспечивается логика генерирования торговых сигналов и построения на
        освное DataFrame с позициями кривой капитала (то есть роста активов) — 
        суммы позиций и доступных средств, доходов/убытков во временной период бара..
        """
        raise NotImplementedError("Should implement backtest()!")
    
    @abstractmethod
    def get_portfolio(self):
        """
        Возвращает DataFrame портфолиома
        """
        raise NotImplementedError("Should implement get_portfolio()!")
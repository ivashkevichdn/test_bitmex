#bin/bash

# Путь к каталогу кэша
PATH_CACHE='./cachebitmex'
# Дата начала сессии
START_SESSION=2018-6-1
# Дата конца сессии
END_SESSION=2018-9-1
# Начальный капитал
CAPITAL=100000

# Если не существует такой папки создадим
if [ ! -f $PATH_CACHE ]; then
    mkdir $PATH_CACHE
fi

# ===== Запуска бек-теста на Pandas ======
python3 pandas_ma_crossover.py


# ===== Запуска бек-теста на Catalyst ======
# Запишем данные курсов с bitmex в файл bitmex_2018-6-1_2018-9-1.csv запустив скрипт
python3 datareaderbitmex.py

# Для того, что бы бэк-тест прошел, изменим название биржи bitmex на gdax (своеобразный кастыль)
catalyst ingest-exchange -x gdax --csv bitmex_2018-6-1_2018-9-1.csv -f minute

# И сам бэк-тест на Catalust.
# Есть два метода:

# 1 метод - через сам скрипт python
python3 catalyst_ma_crossover.py

# 2 метод - через командную строку
#catalyst run -f catalyst_ma_crossover.py -x gdax --start $START_SESSION --end $END_SESSION -c usd --capital-base $CAPITAL

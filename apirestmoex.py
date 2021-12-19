import requests
import json
import time
from enum import Enum
import os

from pyspark.sql import SparkSession  # DataFrame
from pyspark.sql.types import StructField, StringType, StructType, DoubleType, IntegerType
import pyspark.sql.functions as funsp

""" Script выгружает, данные с биржи MOEX, которые хоть как-то могут быть интересны инвесторам,
  и конечно по некоторым моментам могут возникнуть вопросы, но он уже может вытянуть и сохранить
  кучу разрозненных файлов по различным инструментам. Которые планируется преобразовать в полезную
  информацию с помощью pyspark в следующей главе."""

yeas_sec = 31536000  # количество секунд в году


def today(sec=0):
    """ Метод возвращает сегодняшнюю дату или можно отматать, от сегодняшней даты,
        в формате данных, с которыми будет вестись работа. """
    time_time = time.time() - sec
    time_localtime = time.localtime(time_time)
    time_str = time.strftime('%Y-%m-%d', time_localtime)
    return time_str


def data_get(data_set_file):
    #  Это временное решение, для получения последней даты из загруженного фрейма.
    #  На мой взгляд выглядит жутко прикручивать целый движок для такой задачи,
    #  переделаю через библиотеку csv.
    #  Ну уже очень хотелось, что б уже что-то работало, через spark.
    # df = ''  # Да, не используется, но были случаи когда в переменную сложной структуры, что-то где-то прилипало.
    #  schema = StructType([StructField('BOARDID', StringType()),
    #                       StructField('SHORTNAME', StringType()),
    #                       StructField('NAME', StringType()),
    #                       StructField('CLOSE', DoubleType()),
    #                       StructField('OPEN', DoubleType()),
    #                       StructField('HIGH', DoubleType()),
    #                       StructField('LOW', DoubleType()),
    #                       StructField('VALUE', IntegerType()),
    #                       StructField('DURATION', IntegerType()),
    #                       StructField('YIELD', IntegerType()),
    #                       StructField('DECIMALS', IntegerType()),
    #                       StructField('CAPITALIZATION', IntegerType()),
    #                       StructField('CURRENCYID', StringType()),
    #                       StructField('DIVISOR', DoubleType()),
    #                       StructField('TRADINGSESSION', IntegerType()),
    #                       StructField('VOLUME', StringType())])
    spark = SparkSession.builder.getOrCreate()
    df = (spark.read.format("csv")
          .option("header", 'true')
          .option("delimiter", ";")
          .option("inferSchema", "true")
          #  .option('mode', 'DROPMALFORMED')
          #  .schema(schema)
          .load(f'data/enter/{data_set_file}'))
    #  df.printSchema()
    df.select()

    #  Весь метод экспериментальный включая следующую, и столь экзотическое получение даты ради узучательных целей.

    out = df.agg(funsp.max(funsp.to_date('TRADEDATE')).cast(StringType())).collect()[0][0]
    return out


def get_requests(url, params=None, proxys=None, file_name='file.csv'):
    """ Метод выполняет запрос и сохраняет данные в соответствии с ожидаемым форматом. """
    resp = requests.get(url,
                        params=params,
                        proxies=proxys)
    resp_text = resp.text

    #  Решил не пробрасывать через два метода формат ожидаемого ответа, а определять его из строки запроса.
    fd = url.count('.csv')

    #  Обработка форматов в зависимости от приорететности.
    if fd > 0:
        file_res = open(file_name, 'w')
        print(file_res.write(resp_text))
        print()
        file_res.close()
    else:
        fd = url.count('.json')
        if fd > 0:
            return json.loads(resp.text)
        else:
            fd = url.count('.xml')
            if fd > 0:
                print(fd, 'xml')
    #  «ОТМЕЧУ СЕБЕ!!!»  Что-то делать если формат запроса не то что ждали, ли ответ не пришёл.
    return resp


def url_generate(
        num: int = 0,  # Номер шаблона запроса
        engine: str = 'stock',  # Торговая платформа
        market: str = 'shares',  # Выбор рынка для торгов
        board: str = 'TQBR',  # Режим торгов
        security: str = 'SBER',  # Инвестиционный инструмент
        security_arr=None,  # Массив интересующих инвестиционных инструментов
        date_till: str = '',  # Дата, до которой нужны данные
        date_from: str = '',  # Дата, с которой нужны данные
        #  Интервал данных для графика, вообще определен в часах,
        #  но без регистрации меньше чем за сутки данные получить нельзя.
        interval: int = 1,
        #  Данные можно получать начиная с номера записи,
        #  предварительно узнав сколько записей доступно по инструменту
        start: int = 0,  # Формат ответа интересующих данных
        data_format: str = '.json'
):
    """ Метод возвращает http-запрос сгенерированный из параметров, в соответствии с api moex. """

    if security_arr is None:
        security_arr = []

    url_list = [
        # [0] Узнать идентификатор в ИСС для фондового рынка.
        f'http://iss.moex.com/iss/engines{data_format}',

        # [1] Узнать список рынков.
        f'http://iss.moex.com/iss/engines/{engine}/markets{data_format}',

        # [2] Узнать режим торгов.
        f'http://iss.moex.com/iss/engines/{engine}/markets/{market}/boards{data_format}',

        # [3] Если необходимо получить и построить весь справочник рынков и режимов сразу.
        f'http://iss.moex.com/iss{data_format}',

        # [4] Найти требуемый инструмент.
        f'http://iss.moex.com/iss/engines/{engine}/markets/{market}/boards/{board}/securities{data_format}'
        f'?marketdata.securities=off',

        # [5] Найти требуемый инструмент поиском:
        f'http://iss.moex.com/iss/securities{data_format}?q={security}',

        # [6] Запросить итоги торгов за интересующую дату.
        f'http://iss.moex.com/iss/history/engines/{engine}/markets/{market}/boards/{board}/securities/'
        f'{security}{data_format}?from={date_from}&till={date_till}',

        # [7] Описание полей.
        f'http://iss.moex.com/iss/engines/{engine}/markets/{market}/securities/columns{data_format}',

        # [8] Перечень дат, за которые доступна история по инструменту.
        f'http://iss.moex.com/iss/history/engines/{engine}/markets/{market}/boards/{board}/'
        f'securities/{security}/dates{data_format}',

        # [9] Даты и интервалы, для которых доступны данные для графиков.
        f'http://iss.moex.com/iss/history/engines/{engine}/markets/{market}/boards/{board}/securities/{security}/'
        f'candleborders{data_format}',

        # [10] Данные для построения дневных графиков за период.
        f'http://iss.moex.com/iss/history/engines/{engine}/markets/{market}/boards/{board}/securities/{security}/'
        f'candles{data_format}?from={date_from}&till={date_till}&interval={interval}&start={start}',
        #  f'&iss.only=history,history.cursor',

        # [11] Запрос вернет и статическую и динамическую информацию только по указанным индексам.
        f'http://iss.moex.com/iss/engines/{engine}/markets/{market}/securities{data_format}'
        f'?securities={",".join(str(elm) for elm in security_arr)}',

        # [12] Запрос вернет статическую информацию по всем индексам, а текущие динамические параметры – по указанным.
        f'http://iss.moex.com/iss/engines/{engine}/markets/{market}/securities{data_format}'
        f'?marketdata.securities={",".join(str(elm) for elm in security_arr)}'
    ]
    return url_list[num]


def get_engine(engine_arr, proxy_dict=None):
    """ Метод возвращает список торговых площадок. """
    resp_engine = get_requests(url_generate(num=0), proxys=proxy_dict)
    # Запоминаем номер колонки с названием параметра, далее с описанием.
    eng_name = resp_engine['engines']['columns'].index('name')
    eng_title = resp_engine['engines']['columns'].index('title')
    for eng in resp_engine['engines']['data']:
        # Пробегая по массиву данных, всё что относится к фондовому рынку складываем в параметры.
        if eng[eng_title].lower().count('фондовый рынок') > 0:
            engine_arr.append([eng[eng_name]])
    return engine_arr


def get_market(eng, proxy_dict=None):
    """ Метод добавляет к торговой площадке словарь с соответствующими рынками. """
    # Надо узнать какие рынки есть на интересующих нас площадка
    eng.append({})
    resp_market = get_requests(url_generate(num=1, engine=eng[0]), proxys=proxy_dict)
    # Запоминаем номер колонки с названием рынка, далее с описанием.
    mar_name = resp_market['markets']['columns'].index('NAME')
    mar_title = resp_market['markets']['columns'].index('title')
    for mar in resp_market['markets']['data']:
        # Находим рынок акций, индексов и облигаций и записываем их в подготовленный словарь,
        # соотносящийся с торговой площадкой. Список не подойдет, специфика обработки данных инструментов
        # может отличаться, и хотя я излазил весь api вдоль и поперёк и знаю что откуда придет, хочется
        # сохранить вид абстракции и вид что я не ведаю что ж придет и в какой последовательности.
        if 'индекс' in mar[mar_title].lower():
            eng[1]['idx'] = [mar[mar_name]]
            continue
        elif 'акци' in mar[mar_title].lower():
            eng[1]['aks'] = [mar[mar_name]]
            continue
        elif 'облигаци' in mar[mar_title].lower():
            eng[1]['obl'] = [mar[mar_name]]
    return eng[1]


def get_boards_shar(eng, proxy_dict=None):
    """ Метод собирает все актуальные режимы торговли акциями. """
    eng[1]['aks'].append([])
    resp_bonds = get_requests(url_generate(num=2, engine=eng[0], market=eng[1]['aks'][0]), proxys=proxy_dict)
    bon_name = resp_bonds['boards']['columns'].index('boardid')
    bon_title = resp_bonds['boards']['columns'].index('title')
    for bon in resp_bonds['boards']['data']:
        # И снова постигая что-то новое и анализируя информацию, решаем что эти режимы нам не нужны.
        s = bon[bon_title].lower()
        if any(sub in s for sub in ['паи', 'пир', 'рдр', 'лот', 'usd', 'eur']):
            continue
        # А эти нужны.
        if 'т+' in bon[bon_title].lower():
            if 'акции' in bon[bon_title].lower() or 'etf' in bon[bon_title].lower():
                eng[1]['aks'][1].append([bon[bon_name], []])
    return eng[1]['aks'][1]


def get_boards_ind(eng, proxy_dict=None):
    """ Метод собирает все актуальные режимы торговли индексами. """
    eng[1]['idx'].append([])
    resp_bonds = get_requests(url_generate(num=2, engine=eng[0], market=eng[1]['idx'][0]), proxys=proxy_dict)
    bon_name = resp_bonds['boards']['columns'].index('boardid')
    # bon_title = resp_bonds['boards']['columns'].index('title')
    for bon in resp_bonds['boards']['data']:
        eng[1]['idx'][1].append([bon[bon_name], []])
    # А индексы, да пусть будет всЁ.
    return eng[1]['idx'][1]


def get_boards_bon(eng, proxy_dict=None):
    """ Метод собирает все актуальные режимы торговли облигациями. """
    eng[1]['obl'].append([])
    resp_bonds = get_requests(url_generate(num=2, engine=eng[0], market=eng[1]['obl'][0]), proxys=proxy_dict)
    bon_name = resp_bonds['boards']['columns'].index('boardid')
    bon_title = resp_bonds['boards']['columns'].index('title')
    for bon in resp_bonds['boards']['data']:
        # А эти облигации сомнительны.
        if any(sub in bon[bon_title].lower() for sub in ['(', 'пир', 'вне']):
            continue
        # Тут все надежное, наверное.
        if 'т+' in bon[bon_title].lower() or 'etc' in bon[bon_title].lower():
            eng[1]['obl'][1].append([bon[bon_name], []])
    return eng[1]['obl'][1]


def get_tools(eng, mar, bon, tool_var, proxy_dict=None):
    # Сервис отдаёт данные порциями, согласно документации,
    # нужно будет как-то ориентироваться когда тот момент,
    # что мы выбрали всё и пора переходить на следующий инструмент.
    # Делать это будем по дате и введем две переменные с начальным значением год назад.
    control_date = today(yeas_sec)  # Будем контролировать вернувшуюся дату
    temp_data_end = today(yeas_sec)  # А сюда вставлять дату начала запроса
    a = 0  # Сбрасываем номер файла с данными по инструменту
    der = f'{eng}_{mar}_{bon}_{tool_var}_'  # Создаём имя файла
    # Фактически за три прохода можно выбрать всю информацию по инструменту, но если что пойдет не так сделаем шесть
    # проходов, если цикл не прервется раньше по условиям, то и не стоит оно того
    for x_int in range(1):
        temp_list = []
        file_in_dir = os.listdir(path='data/enter')
        for inx in file_in_dir:
            if inx.find(der) != -1:
                temp_list.append(inx)
        temp_list.sort()
        print(temp_list)
        if len(temp_list) > 0:
            # И вот если у нас уже есть файлы с информацией по нашему инструменту.
            # Инкрементируем а, дата полученную в прошлый раз, сохраняем для контроля,
            # и получаем дату последней записи.
            temp_in = temp_list[len(temp_list) - 1]
            temp_a = int(temp_in[len(temp_in) - 9:len(temp_in) - 4:1])
            a = temp_a + 1
            control_date = temp_data_end
            temp_data_end = data_get(temp_in)

            # Последняя публикация итогов торгов вчера или в пятницу, если вчера не пятница.
            # А также стоит учесть случаи когда полученная такая же что и в предыдущем запросе,
            # значит данные больше не публикуются, и случай что нет записей.
            # Все это является весомой причиной перейти к следующему инструменту.
            if time.ctime(time.time())[0:3:1] == 'Mon':
                if temp_data_end == today(60 * 60 * 72):
                    print('break')
                    break
            if time.ctime(time.time())[0:3:1] == 'Sun':
                if temp_data_end == today(60 * 60 * 48):
                    print('break')
                    break
            if temp_data_end == today(60 * 60 * 24):
                print('break')
                break
            if temp_data_end == control_date:
                print('break')
                break
            if temp_data_end is None:
                print('break')
                break
        else:
            # В этом else смыла нет, но если кто-то удаляет файлы в момент скачивания датасета,
            # целостность не потеряется.
            temp_data_end = today(yeas_sec)

        der = der + f'{a:05}'
        get_requests(url_generate(num=10, engine=eng, market=mar, board=bon, security=tool_var,
                                  data_format='.csv', date_till=today(), date_from=temp_data_end),
                     proxys=proxy_dict,
                     file_name=f'data/enter/temp_{der}.csv')

        # Удаляем две первые строки из файла.
        new_temp_file = open(f'data/enter/temp_{der}.csv', 'r')
        new_file = open(f'data/enter/{der}.csv', 'w')
        write_file_csv = 0
        for rfc in new_temp_file:
            if write_file_csv > 1:
                new_file.write(rfc)
                write_file_csv += 1
        new_file.close()
        new_temp_file.close()
        os.remove(f'data/enter/temp_{der}.csv')

        time.sleep(0.05)  # чтоб не подумали что мы участники DDoS атаки
        print('der/', der)

    print('end')

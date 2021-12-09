import requests
import json
import time
import proxy
import os

from pyspark.sql import SparkSession  # DataFrame
from pyspark.sql.types import StructField, StringType, StructType, DoubleType, IntegerType
import pyspark.sql.functions as funsp

""" Script выгружает, данные с биржи MOEX, которые хоть как-то могут быть интересны инвесторам,
  и конечно по некоторым моментам могут возникнуть вопросы, но он уже может вытянуть и сохранить
  кучу разрозненных файлов по различным инструментам. Которые планируется преобразовать в полезную
  информацию с помощью pyspark в следующей главе."""


def data_get(data_set_file):
    #  Это временное решение, для получения последней даты из загруженного фрейма.
    #  На мой взляд выглядит жутко прикручивать целый движок для такой задачи,
    #  переделаю через библиотеку csv.
    #  Ну уже очень хотелось, что б уже что-то работало, через spark.
    df = ''  # Да, не используется, но были случаи когда в переменную сложной структуры, что-то где-то прилипало.
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


def option_main():
    """ Метод задает начальный опции, нужно или включить proxy, что стоит указать в модуле proxy.py
     и проверяет есть или у нас необходимая информация для запросов."""

    #  «ОТМЕЧУ СЕБЕ!!!» что достоверность информации не проверяется и надо обдумать этот момент.

    if proxy.proxy_use:
        proxy_lict = {
            "http": proxy.http_proxy,
            "https": proxy.https_proxy,
            "ftp": proxy.ftp_proxy
        }
    else:
        proxy_lict = None

    if os.listdir(path='.').count('file.json') > 0:
        run = False
    else:
        run = True
    print('proxy.proxy_use:', proxy.proxy_use)
    print('run_enter:', run)
    return proxy_lict, run


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


def array_in_str(modify_array):
    """ Метод переводит массив в строку, набор инструментов удобно обрабатывать в массиве,
        а передавать в запрос нужно старкой. """
    output_str = ''
    for i in modify_array:
        output_str += ',' + i
    output_str = output_str[1:len(output_str):1]
    return output_str


def today(sec=0):
    """ Метод возвращает сегодняшнюю дату или можно отматать, от сегодняшней даты,
        в формате данных, с которыми будет вестись работа. """
    time_time = time.time() - sec
    time_localtime = time.localtime(time_time)
    time_str = time.strftime('%Y-%m-%d', time_localtime)
    return time_str


def yea_old():
    time_str = today(31536000)
    return time_str


def url_generate(
        num: int = 0,  # Номер шаблона запроса
        engine: str = 'stock',  # Торговая платформа
        market: str = 'shares',  # Выбор рынка для торгов
        board: str = 'TQBR',  # Режим торгов
        security: str = 'SBER',  # Инвестиционный инструмент
        security_arr: list = [],  # Массив интересующих инвестиционных инструментов
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
    """ Метод возвращает http-запрос сгенерированный из параметров, в соответствии с api moex """

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
        f'?securities={array_in_str(security_arr)}',

        # [12] Запрос вернет статическую информацию по всем индексам, а текущие динамические параметры – по указанным.
        f'http://iss.moex.com/iss/engines/{engine}/markets/{market}/securities{data_format}'
        f'?marketdata.securities={array_in_str(security_arr)}'
    ]
    return url_list[num]


def run_module():

    #  Соглашаемся с настройками proxy и нужно ли нам загружать параметры или они уже есть.
    proxyDict, run_enter = option_main()

    if run_enter:
        # Запрашиваем или обновляем параметры
        # Сюда будем складывать торговые платформы,
        # по факту нас интересует одна, но вдруг захочется добавить ещё.
        engine_arr = []
        resp_engine = get_requests(url_generate(num=0), proxys=proxyDict)
        # Запоминаем номер колонки с названием параметра, далее с описанием.
        eng_name = resp_engine['engines']['columns'].index('name')
        eng_title = resp_engine['engines']['columns'].index('title')
        # «ОТМЕЧУ СЕБЕ!!!» Добавить исключение
        for eng in resp_engine['engines']['data']:
            # Пробегая по массиву данных, всё что относится к фондовому рынку складываем в параметры.
            if eng[eng_title].lower().count('фондовый рынок') > 0:
                engine_arr.append([eng[eng_name], {}])
        # «ОТМЕЧУ СЕБЕ!!!» Эту ветку развития событий надо будет продумать.
        if not engine_arr:
            print('Фондовый рынок не доступен')

        for eng in engine_arr:
            # Надо узнать какие рынки есть на интересующих нас площадка
            resp_market = get_requests(url_generate(num=1, engine=eng[0]), proxys=proxyDict)
            # Запоминаем номер колонки с названием рынка, далее с описанием.
            mar_name = resp_market['markets']['columns'].index('NAME')
            mar_title = resp_market['markets']['columns'].index('title')
            # «ОТМЕЧУ СЕБЕ!!!» Добавить исключение
            for mar in resp_market['markets']['data']:
                # Находим рынок акций, индексов и облигаций и записываем их в подготовленный словарь словарь,
                # соотносящийся с торговой площадкой. Список не подойдет, специфика обработки данных инструментов
                # может отличаться, и хотя я излазил весь api вдоль и поперёк и знаю что откуда придет, хочется
                # сохранить вид абстракции и вид что я не ведаю что ж придет и в какой последовательности.
                if mar[mar_title].lower().count('индекс') > 0:
                    eng[1]['idx'] = [mar[mar_name], []]
                    continue
                elif mar[mar_title].lower().count('акци') > 0:
                    eng[1]['aks'] = [mar[mar_name], []]
                    continue
                elif mar[mar_title].lower().count('облигаци') > 0:
                    eng[1]['obl'] = [mar[mar_name], []]

        # Теперь находим параметры торговых режимов, немного поизучав что это такое,
        # узнал что у разных групп инструментов они разные, так что и обработку мы
        # будем вести отдельно, и главное не запутаться в подстановках.
        for eng in engine_arr:
            resp_bonds = get_requests(url_generate(num=2, engine=eng[0], market=eng[1]['aks'][0]), proxys=proxyDict)
            bon_name = resp_bonds['boards']['columns'].index('boardid')
            bon_title = resp_bonds['boards']['columns'].index('title')
            # «ОТМЕЧУ СЕБЕ!!!» Добавить исключение
            for bon in resp_bonds['boards']['data']:
                # И снова постигая что-то новое и анализируя информацию, решаем что эти режимы нам не нужны.
                if bon[bon_title].lower().count('паи') > 0:
                    continue
                if bon[bon_title].lower().count('пир') > 0:
                    continue
                if bon[bon_title].lower().count('рдр') > 0:
                    continue
                if bon[bon_title].lower().count('лот') > 0:
                    continue
                if bon[bon_title].lower().count('usd') > 0:
                    continue
                if bon[bon_title].lower().count('eur') > 0:
                    continue
                # А эти нужны.
                if bon[bon_title].lower().count('т+') > 0:
                    if bon[bon_title].lower().count('акции') > 0:
                        eng[1]['aks'][1].append([bon[bon_name], []])
                        continue
                    if bon[bon_title].lower().count('etf') > 0:
                        eng[1]['aks'][1].append([bon[bon_name], []])

        for eng in engine_arr:
            resp_bonds = get_requests(url_generate(num=2, engine=eng[0], market=eng[1]['idx'][0]), proxys=proxyDict)
            bon_name = resp_bonds['boards']['columns'].index('boardid')
            bon_title = resp_bonds['boards']['columns'].index('title')
            # «ОТМЕЧУ СЕБЕ!!!» Добавить исключение
            for bon in resp_bonds['boards']['data']:
                eng[1]['idx'][1].append([bon[bon_name], []])
            # А индексы, да пусть будет всЁ.

        for x_int in engine_arr:
            resp_bonds = get_requests(url_generate(num=2, engine=x_int[0], market=x_int[1]['obl'][0]), proxys=proxyDict)
            bon_name = resp_bonds['boards']['columns'].index('boardid')
            bon_title = resp_bonds['boards']['columns'].index('title')
            # «ОТМЕЧУ СЕБЕ!!!» Добавить исключение
            for y_int in resp_bonds['boards']['data']:
                # А эти облигации сомнительны.
                if y_int[bon_title].lower().count('(') > 0:
                    continue
                if y_int[bon_title].lower().count('пир') > 0:
                    continue
                if y_int[bon_title].lower().count('вне') > 0:
                    continue
                # Тут все надежное, наверное.
                if y_int[bon_title].lower().count('т+') > 0:
                    x_int[1]['obl'][1].append([y_int[bon_name], []])
                    continue
                if y_int[bon_title].lower().count('etc') > 0:
                    x_int[1]['obl'][1].append([y_int[bon_name], []])

        # Ну и наконец, можем собрать доступные инвестиционные инструменты в соответствии с режимами торгов,
        # соответствующих рынков, соответственных платформ.
        for eng in engine_arr:
            for mar_key in eng[1]:
                mar = (eng[1][mar_key])
                for bon in mar[1]:
                    resp_tool = get_requests(url_generate(num=4, engine=eng[0], market=mar[0], board=bon[0]),
                                             proxys=proxyDict)
                    sec_name = resp_tool['securities']['columns'].index('SECID')
                    # «ОТМЕЧУ СЕБЕ!!!» Добавить исключение
                    for sec_id in resp_tool['securities']['data']:
                        bon[1].append(sec_id[sec_name])
        # И это было не просто, так что сохраню ка я все параметры в файл.
        # Чтоб не потерялось.
        list_requests = open('file.json', 'w')
        engine_json = json.dumps(engine_arr)
        list_requests.write(str(engine_json))
        list_requests.close()
    else:
        # И если у нас есть файл с параметрами, и нам не страшно,
        # вдруг там кто-то что-то поправил или на бирже что-то поменялось, то открываем и читаем.
        list_requests = open('file.json', 'r')
        list_read = list_requests.read()
        list_requests.close()
        engine_arr = json.loads(list_read)

    var_one, var_load = True, False  # Для прерывания while, который стоит заменить for, оставлю пометку ВАЖНО!!!
    # Раскладываем параметры на составное
    for eng in engine_arr:
        for mar_key in eng[1]:
            mar = (eng[1][mar_key])
            for bon in mar[1]:
                for tool_var in bon[1]:
                    if var_one:  # это к while
                        var_load = True  # тоже while
                    # Сервис отдаёт данные порциями, согласно документации,
                    # нужно будет как-то ориентироваться когда тот момент,
                    # что мы выбрали всё и пора переходить на следующий инструмент.
                    # Делать это будем по дате и введем две переменные с начальным значением год назад.
                    control_date = yea_old()  # Будем контролировать вернувшуюся дату
                    temp_data_end = yea_old()  # А сюда вставлять дату начала запроса
                    a = 0  # Сбрасываем номер файла с данными по инструменту
                    der = f'{eng[0]}_{mar[0]}_{bon[0]}_{tool_var}_'  # Создаём имя файла
                    while var_load:
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
                            print(temp_in)
                            control_date = temp_data_end
                            temp_data_end = data_get(temp_in)
                            print('temp_data_end', temp_data_end)
                            print('control_date', control_date)
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
                            temp_data_end = yea_old()

                        der = der + f'{a:05}'
                        get_requests(url_generate(num=10, engine=eng[0], market=mar[0], board=bon[0], security=tool_var,
                                                  data_format='.csv', date_till=today(), date_from=temp_data_end),
                                     proxys=proxyDict,
                                     file_name=f'data/enter/temp_{der}.csv')

                        # Удаляем две первые строки из файла.
                        new_temp_file = open(f'data/enter/temp_{der}.csv', 'r')
                        new_file = open(f'data/enter/{der}.csv', 'w')
                        write_file_csv = 0
                        for rfc in new_temp_file:
                            if write_file_csv > 1:
                                new_file.write(rfc)
                                write_file_csv += 1
                        new_temp_file.close()
                        new_file.close()
                        os.remove(f'data/enter/temp_{der}.csv')

                        # отладка
                        entr = ''  # input('>>>')
                        if entr == 'q':
                            var_one, var_load = False, False
                        # отладка

                        time.sleep(0.05)  # чтоб не подумали что мы участники DDoS атаки
                        print('der/', der)
    print('end')

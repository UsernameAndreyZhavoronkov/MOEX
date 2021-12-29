import analtydata

import requests
import json
import time
import os
from enum import Enum

yeas_sec = 16000000  # количество секунд в году
range_tool = 6  # Максимально количество запросов данных для графиков по одному торговому инструменту


class ApiMethod(Enum):
    GET_ISS = 0
    GET_MARKETS = 1
    GET_BOARDS = 2
    GET_ALL = 3
    GET_TOOL = 4
    GET_SEC_QUEST = 5
    GET_TRADEDATE = 6
    GET_COLUMNS = 7
    GET_AVA_HIST = 8
    GET_AVA_CANDLE = 9
    GET_CANDLES = 10
    GET_INFO_INXS = 11
    GET_INFO_INXS_X = 12


def today(sec=0):
    """ Метод возвращает сегодняшнюю дату по умолчанию или дату какое-то количество секунд назад,
        в формате данных, с которыми будет вестись работа. """

    time_time = time.time() - sec
    time_localtime = time.localtime(time_time)
    time_str = time.strftime('%Y-%m-%d', time_localtime)
    return time_str


def check_date(temp_data_end):
    """ Метод возвращает 'True', если переданная ему очень вероятно является последней,
        за которую опубликованы результаты торгов. """

    return ((time.ctime(time.time())[0:3:1] == 'Mon' and temp_data_end == today(60 * 60 * 72))
            or (time.ctime(time.time())[0:3:1] == 'Sun' and temp_data_end == today(60 * 60 * 48))
            or temp_data_end == today(60 * 60 * 24)
            or temp_data_end == 'NOT_ACTIVE')


def get_requests(
        method,  # Номер шаблона запроса
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
        data_format: str = '.json',
        proxy_s=None
):
    """ Метод возвращает текст ответа на http-запрос сгенерированный из параметров, в соответствии с api moex. """

    if security_arr is None:
        security_arr = []

    url_list = {
        # [0] Узнать идентификатор в ИСС для фондового рынка.
        ApiMethod.GET_ISS: f'http://iss.moex.com/iss/engines{data_format}',

        # [1] Узнать список рынков.
        ApiMethod.GET_MARKETS: f'http://iss.moex.com/iss/engines/{engine}/markets{data_format}',

        # [2] Узнать режим торгов.
        ApiMethod.GET_BOARDS: f'http://iss.moex.com/iss/engines/{engine}/markets/{market}/boards{data_format}',

        # [3] Если необходимо получить и построить весь справочник рынков и режимов сразу.
        ApiMethod.GET_ALL: f'http://iss.moex.com/iss{data_format}',

        # [4] Найти требуемый инструмент.
        ApiMethod.GET_TOOL: f'http://iss.moex.com/iss/engines/{engine}/markets/{market}/boards/{board}/'
                            f'securities{data_format}?marketdata.securities=off',

        # [5] Найти требуемый инструмент поиском:
        ApiMethod.GET_SEC_QUEST: f'http://iss.moex.com/iss/securities{data_format}?q={security}',

        # [6] Запросить итоги торгов за интересующую дату.
        ApiMethod.GET_TRADEDATE: f'http://iss.moex.com/iss/history/engines/{engine}/markets/{market}/boards/{board}/'
                                 f'securities/{security}{data_format}?from={date_from}&till={date_till}',

        # [7] Описание полей.
        ApiMethod.GET_COLUMNS: f'http://iss.moex.com/iss/engines/{engine}/markets/{market}/'
                               f'securities/columns{data_format}',

        # [8] Перечень дат, за которые доступна история по инструменту.
        ApiMethod.GET_AVA_HIST: f'http://iss.moex.com/iss/history/engines/{engine}/markets/{market}/boards/{board}/'
                                f'securities/{security}/dates{data_format}',

        # [9] Даты и интервалы, для которых доступны данные для графиков.
        ApiMethod.GET_AVA_CANDLE: f'http://iss.moex.com/iss/history/engines/{engine}/markets/{market}/boards/{board}/'
                                  f'securities/{security}/candleborders{data_format}',

        # [10] Данные для построения дневных графиков за период.
        ApiMethod.GET_CANDLES: f'http://iss.moex.com/iss/history/engines/{engine}/markets/{market}/boards/{board}/'
                               f'securities/{security}/candles{data_format}?from={date_from}&till={date_till}'
                               f'&interval={interval}&start={start}',

        # [11] Запрос вернет и статическую и динамическую информацию только по указанным индексам.
        ApiMethod.GET_INFO_INXS: f'http://iss.moex.com/iss/engines/{engine}/markets/{market}/securities{data_format}'
                                 f'?securities={",".join(str(elm) for elm in security_arr)}',

        # [12] Запрос вернет статическую информацию по всем индексам, а текущие динамические параметры – по указанным.
        ApiMethod.GET_INFO_INXS_X: f'http://iss.moex.com/iss/engines/{engine}/markets/{market}/securities{data_format}'
                                   f'?marketdata.securities={",".join(str(elm) for elm in security_arr)}'
    }
    return requests.get(url_list[method], proxies=proxy_s).text


def get_engine(engine_arr, proxy_dict=None):
    """ Метод возвращает список торговых площадок. """

    resp_engine = json.loads(get_requests(ApiMethod.GET_ISS, proxy_s=proxy_dict))

    # Запоминаем номер колонки с названием параметра, далее с описанием.
    eng_name = resp_engine['engines']['columns'].index('name')
    eng_title = resp_engine['engines']['columns'].index('title')

    # Пробегая по массиву данных, всё что относится к фондовому рынку складываем в параметры.
    for eng in resp_engine['engines']['data']:
        if eng[eng_title].lower().count('фондовый рынок') > 0:
            engine_arr.append([eng[eng_name]])
    return engine_arr


def get_market(eng, proxy_dict=None):
    """ Метод добавляет к торговой площадке словарь с соответствующими рынками. """

    # Надо узнать какие рынки есть на интересующих нас площадка.
    eng.append({})
    resp_market = json.loads(get_requests(ApiMethod.GET_MARKETS, engine=eng[0], proxy_s=proxy_dict))
    # Запоминаем номер колонки с названием рынка, далее с описанием.
    mar_name = resp_market['markets']['columns'].index('NAME')
    mar_title = resp_market['markets']['columns'].index('title')

    # Находим рынок акций, индексов и облигаций и записываем их в подготовленный словарь,
    # соотносящийся с торговой площадкой. Список не подойдет, специфика обработки данных инструментов
    # может отличаться, и хотя я излазил весь api вдоль и поперёк и знаю что откуда придет, хочется
    # сохранить вид абстракции и вид что я не ведаю что ж придет и в какой последовательности.
    for mar in resp_market['markets']['data']:
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
    resp_bonds = json.loads(get_requests(ApiMethod.GET_BOARDS, engine=eng[0],
                                         market=eng[1]['aks'][0], proxy_s=proxy_dict))
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
    resp_bonds = json.loads(get_requests(ApiMethod.GET_BOARDS, engine=eng[0],
                                         market=eng[1]['idx'][0], proxy_s=proxy_dict))
    bon_name = resp_bonds['boards']['columns'].index('boardid')
    # bon_title = resp_bonds['boards']['columns'].index('title')
    for bon in resp_bonds['boards']['data']:
        eng[1]['idx'][1].append([bon[bon_name], []])
    # А индексы, да пусть будет всЁ.
    return eng[1]['idx'][1]


def get_boards_bon(eng, proxy_dict=None):
    """ Метод собирает все актуальные режимы торговли облигациями. """

    eng[1]['obl'].append([])
    resp_bonds = json.loads(get_requests(ApiMethod.GET_BOARDS, engine=eng[0],
                                         market=eng[1]['obl'][0], proxy_s=proxy_dict))
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
    """ Метод проверяет, выгружали ли мы раньше данные по этому инструменту и полноту этих данных;
        а также при необходимости догружает торговые данные с биржи. """

    f_for_tool = None  # Создадим ссылку, гле будем хранить имя файла с торговыми данными
    go_update_trade = False  # Флаг обновления торговых данных
    temp_data_end = today(yeas_sec)  # Дата начала запроса
    der = f'{eng}_{mar}_{bon}_{tool_var[0]}_'  # Создаём имя файла

    # Поищем нет ли у нас ранние загруженных данных по этому инструменту,
    # если есть узнаем последнюю дату, а если нет поставим метку, что нет.
    file_in_dir = os.listdir(path='data/enter')
    path_corr = os.listdir(path='data/data_for_spark')
    for inx in file_in_dir:
        if der in inx:
            file_frame = inx
            data_end = inx[len(inx) - 10:len(inx):1]
            if data_end > temp_data_end:
                temp_data_end = data_end
            break
    else:
        file_frame = None

    # Если данные за актуальную дату уходим из цикла.
    # В противном случае загружаем данные в фрейм, если они есть.
    if not (check_date(temp_data_end)):
        if not (file_frame is None):
            df_buf = analtydata.get_frame_in_base(f'data/enter/{file_frame}')
        else:
            df_buf = None

        # Собираем информацию запросами.
        for x_int in range(range_tool):
            with open(f'temp/temp_candles{x_int}.csv', 'w') as tmp_file:
                tmp_file.write(get_requests(ApiMethod.GET_CANDLES, engine=eng, market=mar, board=bon,
                                            security=tool_var[0], data_format='.csv', date_till=today(),
                                            date_from=temp_data_end, proxy_s=proxy_dict))

            # Добавляем информацию в датафрейм.
            df_buf, control_data = analtydata.get_new_file_tool(data_frame=df_buf, f=f'temp/temp_candles{x_int}.csv')

            # Проверяем дату и если у нас нет причин продолжать сохраняем что есть и уходим.
            if check_date(control_data) or temp_data_end == control_data or control_data is None:
                go_update_trade = True
                if control_data is None:
                    control_data = 'NOT_ACTIVE'
                    go_update_trade = False
                if temp_data_end == control_data:
                    go_update_trade = False
                f_for_tool = der + control_data
                analtydata.save_data(df_buf, der + control_data)
                break
            else:
                temp_data_end = control_data
    elif not ('in_corr__00-00-00' in path_corr) and temp_data_end != 'NOT_ACTIVE':
        go_update_trade = True
        f_for_tool = der + temp_data_end
    return f_for_tool, go_update_trade

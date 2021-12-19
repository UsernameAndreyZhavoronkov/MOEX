import time
import json
import apirestmoex
import analtydata
import proxy
import os


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

    # Обновляем записи о рынках, раз в месяц. Не думаю что они будут часто меняться.
    file = time.ctime()[4:7:1]
    dir_trade = os.listdir(path='data/trade')
    if f'{file}.json' in dir_trade:
        run = False
    else:
        for rem_file in dir_trade:
            os.remove(path=f'data/trade/{rem_file}')
        run = True
    return proxy_lict, run


if __name__ == '__main__':
    if not ('data' in os.listdir(path='.')):
        os.mkdir('data')
    if not ('enter' in os.listdir(path='data')):
        os.mkdir('data/enter')
    if not ('data_for_spark' in os.listdir(path='data')):
        os.mkdir('data/data_for_spark')
    if not ('trade' in os.listdir(path='data')):
        os.mkdir('data/trade')

    #  Соглашаемся с настройками proxy и нужно ли нам загружать параметры или они уже есть.
    proxyDict, run_enter = option_main()

    # Сюда будем складывать торговые платформы,
    # по факту нас интересует одна, но вдруг захочется добавить ещё.
    engine_arr = []

    if run_enter:
        # Запрашиваем или обновляем параметры
        engine_arr = apirestmoex.get_engine(engine_arr, proxyDict)

        if not engine_arr:
            print('Фондовый рынок не доступен, попробуйте позже.')
        for eng in engine_arr:
            apirestmoex.get_market(eng, proxyDict)

        # Теперь находим параметры торговых режимов, немного поизучав что это такое,
        # узнал что у разных групп инструментов они разные, так что и обработку мы
        # будем вести отдельно, и главное не запутаться в подстановках.
        for eng in engine_arr:
            apirestmoex.get_boards_shar(eng, proxyDict)
        for eng in engine_arr:
            apirestmoex.get_boards_ind(eng, proxyDict)
        for eng in engine_arr:
            apirestmoex.get_boards_bon(eng, proxyDict)

        # Ну и наконец, можем собрать доступные инвестиционные инструменты в соответствии с режимами торгов,
        # соответствующих рынков, соответственных платформ.
        for eng in engine_arr:
            for mar_key in eng[1]:
                mar = (eng[1][mar_key])
                for bon in mar[1]:
                    resp_tool = apirestmoex.\
                        get_requests(apirestmoex.url_generate(num=4, engine=eng[0], market=mar[0],
                                                              board=bon[0]), proxys=proxyDict)
                    sec_name = resp_tool['securities']['columns'].index('SECID')
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

    # Раскладываем параметры на составное
    for eng in engine_arr:
        for mar_key in eng[1]:
            mar = (eng[1][mar_key])
            for bon in mar[1]:
                for tool_var in bon[1]:
                    apirestmoex.get_tools(eng=eng[0], mar=mar[0], bon=bon[0], tool_var=tool_var)

    print(engine_arr)

    # test code:

    if engine_arr == [['stock',
                       {'idx': ['index', [['INAV', []], ['MMIX', []], ['RTSI', []], ['SDII', []], ['SNDX', []]]],
                        'aks': ['shares', [['TQBR', []], ['TQDE', []], ['TQLI', []], ['TQLV', []], ['TQTF', []]]],
                        'obl': ['bonds', [['EQTC', []], ['TQCB', []], ['TQNO', []], ['TQOB', []], ['TQOS', []],
                                          ['TQOV', []], ['TQRD', []], ['TQTC', []]]]}]]:
        print(True)
    else:
        print(False)

    print([['stock',
            {'idx': ['index', [['INAV', []], ['MMIX', []], ['RTSI', []], ['SDII', []], ['SNDX', []]]],
             'aks': ['shares', [['TQBR', []], ['TQDE', []], ['TQLI', []], ['TQLV', []], ['TQTF', []]]],
             'obl': ['bonds', [['EQTC', []], ['TQCB', []], ['TQNO', []], ['TQOB', []], ['TQOS', []],
                               ['TQOV', []], ['TQRD', []], ['TQTC', []]]]}]])

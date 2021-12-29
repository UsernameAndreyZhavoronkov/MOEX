import analtydata
import apirestmoex
import config
import dialog
import progres
import proxy

import time
import json
import os
from colorama import Fore, Style, init


def option_main():
    """ Метод задает начальный опции, нужно или включить proxy, что стоит указать в модуле proxy.py
     и проверяет есть или у нас необходимая информация для запросов."""

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
    if not (os.name == 'posix'):
        init(convert=True)
    print(Style.BRIGHT, Fore.CYAN)
    print('Проверяем директории ...')
    if not ('data' in os.listdir(path='.')):
        os.mkdir('data')
    if not ('temp' in os.listdir(path='.')):
        os.mkdir('temp')
    if not ('enter' in os.listdir(path='data')):
        os.mkdir('data/enter')
    if not ('data_for_spark' in os.listdir(path='data')):
        os.mkdir('data/data_for_spark')
    if not ('trade' in os.listdir(path='data')):
        os.mkdir('data/trade')
    if not ('graphic' in os.listdir(path='data')):
        os.mkdir('data/graphic')

    #  Соглашаемся с настройками proxy и нужно ли нам загружать параметры или они уже есть.
    proxyDict, run_enter = option_main()

    # Сюда будем складывать торговые платформы,
    # по факту нас интересует одна, но вдруг захочется добавить ещё.
    engine_arr = []

    print('Собираем информацию о торговых режимах ...')
    progress_ = 0
    print(Fore.LIGHTYELLOW_EX, end='')
    progres.update_progress(progress_)
    if run_enter:
        # Запрашиваем список торговых площадок
        engine_arr = apirestmoex.get_engine(engine_arr, proxyDict)

        for eng in engine_arr:
            apirestmoex.get_market(eng, proxyDict)

        # Теперь находим параметры торговых режимов, немного поизучав, что это такое,
        # узнал что у разных групп инструментов они разные, так что и обработку мы
        # будем вести отдельно, и главное не запутаться в подстановках.
        for eng in engine_arr:
            apirestmoex.get_boards_shar(eng, proxyDict)
            progress_ = len(eng[1]['aks'][1])
        for eng in engine_arr:
            apirestmoex.get_boards_ind(eng, proxyDict)
            progress_ += len(eng[1]['idx'][1])
        for eng in engine_arr:
            apirestmoex.get_boards_bon(eng, proxyDict)
            progress_ += + len(eng[1]['obl'][1])

        # Ну и наконец, можем собрать доступные инвестиционные инструменты в соответствии с режимами торгов,
        # соответствующих рынков, соответственных платформ.
        percent = 0
        for eng in engine_arr:
            for mar_key in eng[1]:
                mar = (eng[1][mar_key])
                for bon in mar[1]:
                    percent += 1
                    progres.update_progress(percent/progress_)
                    resp_tool = json.loads(apirestmoex.get_requests(apirestmoex.ApiMethod.GET_TOOL, engine=eng[0],
                                                                    market=mar[0], board=bon[0], proxy_s=proxyDict))
                    sec_name = resp_tool['securities']['columns'].index('SECID')
                    short_name = resp_tool['securities']['columns'].index('SHORTNAME')
                    for sec_id in resp_tool['securities']['data']:
                        bon[1].append([sec_id[sec_name], sec_id[short_name]])

        # И это было не просто, так что сохраню ка я все параметры в файл.
        # Чтоб не потерялось.
        with open(f'data/trade/{time.ctime()[4:7:1]}.json', 'w') as list_requests:
            engine_json = json.dumps(engine_arr)
            list_requests.write(str(engine_json))
    else:
        # И если у нас есть файл с параметрами, и нам не страшно,
        # вдруг там кто-то что-то поправил или на бирже что-то поменялось, то открываем и читаем.
        with open(f'data/trade/{time.ctime()[4:7:1]}.json', 'r') as list_requests:
            list_read = list_requests.read()
            engine_arr = json.loads(list_read)
            progres.update_progress(1)

    progress_ = 0
    progress_ += sum(map(lambda ea: sum(map(lambda x: sum([len(bn[1]) for bn in ea[1][x][1]]), ea[1])), engine_arr))

    print(Fore.CYAN)
    print('Собираем информацию о торговых инструментах ...')
    print(Fore.LIGHTYELLOW_EX, end='')
    progres.update_progress(0)
    trader_data = None  # Ссылка для объекта содержащим дата фреймы для аналитики
    # Раскладываем параметры на составное
    percent = 0
    for eng in engine_arr:
        for mar_key in eng[1]:
            mar = (eng[1][mar_key])
            for bon in mar[1]:
                for tool_var in bon[1]:
                    percent += 1
                    progres.update_progress(percent / progress_)
                    # Ограничение для запросов
                    if config.restriction(eng=eng[0], mar=mar[0], bon=bon[0], tool_var=tool_var[0]):
                        dir_new_data, go_update_trade = apirestmoex.get_tools(eng=eng[0], mar=mar[0], bon=bon[0],
                                                                              tool_var=tool_var, proxy_dict=proxyDict)

                        # Если пришло что-то новое, обрабатываем данные, создаем объект для хранения, если его нет.
                        if go_update_trade:
                            up_data, in_corr, trade_table = analtydata.data_get(path_dir='data/enter/',
                                                                                file_data=dir_new_data)
                            if trader_data is None:
                                trader_data = analtydata.Trader()
                            if up_data:
                                trader_data.update_trade(in_corr, trade_table)

    print(Fore.CYAN)
    print('Анализируем данные, строим торговые стратегии ...')

    if not (trader_data is None):
        trader_data.write_data()

    analtydata.spark.stop()

    # Немного уберём мусор за собой.
    analtydata.data_old.drop_old()
    analtydata.data_old.rename_dir()

    analtydata.spark = analtydata.SparkSession.builder.getOrCreate()

    # Если мы обновляли какие-то расчеты, то стоит их сохранить,
    # если ничего интересного не пришло загрузим данные локально.
    print(Fore.CYAN + 'Ещё немного ...' + Fore.GREEN)
    trader_data = analtydata.Trader()

    # Создадим таблицу зависимостей.
    trader_data.create_table_corr()

    cyc_run = True
    while True:
        cyc_run, cortege = dialog.dialog(cyc_run)
        if not cyc_run:
            break
        if cortege == 'g':
            gr_list = os.listdir('data/graphic')
            dialog.dialog2(gr_list)
        else:
            trader_data.tc(cortege)

    # trader_data.show_df()
    analtydata.spark.stop()

    print('end')

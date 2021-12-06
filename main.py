import requests
import json
import time
import proxy
import os

from pyspark.sql import SparkSession  # DataFrame
from pyspark.sql.types import StructField, StringType, StructType, DoubleType, IntegerType
import pyspark.sql.functions as funsp


def data_get(data_set_file):
    df = ''
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
    out = df.agg(funsp.max(funsp.to_date('TRADEDATE')).cast(StringType())).collect()[0][0]
    return out


def option_main():
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
    resp = requests.get(url,
                        params=params,
                        proxies=proxys)
    resp_text = resp.text

    fd = url.count('.csv')
    if fd > 0:
        file_res = open(file_name, 'w')
        print(file_res.write(resp_text))
        print()
        file_res.close()
    else:
        fd = url.count('.xml')
        if fd > 0:
            print(fd, 'xml')
        else:
            fd = url.count('.json')
            if fd > 0:
                return json.loads(resp.text)
    return resp


def array_in_str(modify_array):
    output_str = ''
    for i in modify_array:
        output_str += ',' + i
    output_str = output_str[1:len(output_str):1]
    return output_str


def today(sec=0):
    time_time = time.time() - sec
    time_localtime = time.localtime(time_time)
    time_str = time.strftime('%Y-%m-%d', time_localtime)
    return time_str


def yea_old():
    time_time = time.time()
    time_localtime = time.localtime(time_time - 31536000)
    time_str = time.strftime('%Y-%m-%d', time_localtime)
    return time_str


def url_generate(
        num: int = 0,
        engine: str = 'stock',
        market: str = 'shares',
        board: str = 'TQBR',
        security: str = 'SBER',
        security_arr: list = [],
        date_till: str = '',
        date_from: str = '',
        interval: int = 1,
        start: int = 0,
        data_format: str = '.json'
):
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


if __name__ == '__main__':

    proxyDict, run_enter = option_main()

    if run_enter:
        engine_arr = []
        resp_engine = get_requests(url_generate(num=0), proxys=proxyDict)
        eng_name = resp_engine['engines']['columns'].index('name')
        eng_title = resp_engine['engines']['columns'].index('title')
        for x_int in resp_engine['engines']['data']:
            if x_int[eng_title].lower().count('фондовый рынок') > 0:
                engine_arr.append([x_int[eng_name], {}])

        if not engine_arr:
            print('Фондовый рынок не доступен')

        for x_int in engine_arr:
            resp_market = get_requests(url_generate(num=1, engine=x_int[0]), proxys=proxyDict)
            mar_name = resp_market['markets']['columns'].index('NAME')
            mar_title = resp_market['markets']['columns'].index('title')
            for y_int in resp_market['markets']['data']:
                if y_int[mar_title].lower().count('индекс') > 0:
                    x_int[1]['idx'] = [y_int[mar_name], []]
                    continue
                elif y_int[mar_title].lower().count('акци') > 0:
                    x_int[1]['aks'] = [y_int[mar_name], []]
                    continue
                elif y_int[mar_title].lower().count('облигаци') > 0:
                    x_int[1]['obl'] = [y_int[mar_name], []]

        for x_int in engine_arr:
            resp_bonds = get_requests(url_generate(num=2, engine=x_int[0], market=x_int[1]['aks'][0]), proxys=proxyDict)
            bon_name = resp_bonds['boards']['columns'].index('boardid')
            bon_title = resp_bonds['boards']['columns'].index('title')
            for y_int in resp_bonds['boards']['data']:
                if y_int[bon_title].lower().count('паи') > 0:
                    continue
                if y_int[bon_title].lower().count('пир') > 0:
                    continue
                if y_int[bon_title].lower().count('рдр') > 0:
                    continue
                if y_int[bon_title].lower().count('лот') > 0:
                    continue
                if y_int[bon_title].lower().count('usd') > 0:
                    continue
                if y_int[bon_title].lower().count('eur') > 0:
                    continue
                if y_int[bon_title].lower().count('т+') > 0:
                    if y_int[bon_title].lower().count('акции') > 0:
                        x_int[1]['aks'][1].append([y_int[bon_name], []])
                        continue
                    if y_int[bon_title].lower().count('etf') > 0:
                        x_int[1]['aks'][1].append([y_int[bon_name], []])

        for x_int in engine_arr:
            resp_bonds = get_requests(url_generate(num=2, engine=x_int[0], market=x_int[1]['idx'][0]), proxys=proxyDict)
            bon_name = resp_bonds['boards']['columns'].index('boardid')
            bon_title = resp_bonds['boards']['columns'].index('title')
            for y_int in resp_bonds['boards']['data']:
                x_int[1]['idx'][1].append([y_int[bon_name], []])

        for x_int in engine_arr:
            resp_bonds = get_requests(url_generate(num=2, engine=x_int[0], market=x_int[1]['obl'][0]), proxys=proxyDict)
            bon_name = resp_bonds['boards']['columns'].index('boardid')
            bon_title = resp_bonds['boards']['columns'].index('title')
            for y_int in resp_bonds['boards']['data']:
                if y_int[bon_title].lower().count('(') > 0:
                    continue
                if y_int[bon_title].lower().count('пир') > 0:
                    continue
                if y_int[bon_title].lower().count('вне') > 0:
                    continue
                if y_int[bon_title].lower().count('т+') > 0:
                    x_int[1]['obl'][1].append([y_int[bon_name], []])
                    continue
                if y_int[bon_title].lower().count('etc') > 0:
                    x_int[1]['obl'][1].append([y_int[bon_name], []])

        for eng in engine_arr:
            for mar_key in eng[1]:
                mar = (eng[1][mar_key])
                for bon in mar[1]:
                    resp_tool = get_requests(url_generate(num=4, engine=eng[0], market=mar[0], board=bon[0]),
                                             proxys=proxyDict)
                    sec_name = resp_tool['securities']['columns'].index('SECID')
                    for sec_id in resp_tool['securities']['data']:
                        bon[1].append(sec_id[sec_name])
                    time.sleep(0.1)
        list_requests = open('file.json', 'w')
        engine_json = json.dumps(engine_arr)
        list_requests.write(str(engine_json))
        list_requests.close()
    else:
        list_requests = open('file.json', 'r')
    list_read = list_requests.read()
    list_requests.close()
    engine_arr = json.loads(list_read)

    var_one, var_load = True, False
    for eng in engine_arr:
        for mar_key in eng[1]:
            mar = (eng[1][mar_key])
            for bon in mar[1]:
                for tool_var in bon[1]:
                    if var_one:
                        var_load = True
                    a = 0
                    control_date = yea_old()
                    temp_data_end = yea_old()
                    while var_load:
                        temp_list = []
                        a += 1
                        der = f'{eng[0]}_{mar[0]}_{bon[0]}_{tool_var}_'  # +f'{a:05}'
                        file_in_dir = os.listdir(path='data/enter')
                        for inx in file_in_dir:
                            if inx.find(der) != -1:
                                temp_list.append(inx)
                        temp_list.sort()
                        print(temp_list)
                        if len(temp_list) > 0:
                            temp_in = temp_list[len(temp_list) - 1]
                            der = temp_in[0:len(temp_in) - 9:1]
                            temp_a = int(temp_in[len(temp_in) - 9:len(temp_in) - 4:1])
                            a = temp_a + 1
                            print(der + f'{temp_a:05}' + '.csv')
                            control_date = temp_data_end
                            temp_data_end = data_get(der + f'{temp_a:05}' + '.csv')
                            print('temp_data_end', temp_data_end)
                            print('control_date', control_date)

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
                            temp_data_end = yea_old()
                        der = der + f'{a:05}'

                        get_requests(url_generate(num=10, engine=eng[0], market=mar[0], board=bon[0], security=tool_var,
                                                  data_format='.csv', date_till=today(), date_from=temp_data_end),
                                     proxys=proxyDict,
                                     file_name=f'data/enter/{der}.csv')

                        new_file = open(f'data/enter/{der}.csv', 'r')
                        read_file_csv = []
                        for rfc in new_file:
                            read_file_csv.append(rfc)
                        new_file.close()
                        new_file = open(f'data/enter/{der}.csv', 'w')
                        for rfc in range(2, len(read_file_csv)):
                            new_file.write(read_file_csv[rfc])
                        new_file.close()

                        entr =  input('>>>')
                        if entr == 'q':
                            var_one, var_load = False, False
                        time.sleep(0.1)

                        print('der/', der)
    print('end')

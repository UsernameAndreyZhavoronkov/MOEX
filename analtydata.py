import apirestmoex
import config

import os
import numpy
import matplotlib
import matplotlib.dates as mpl_dates
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter, DayLocator, WeekdayLocator
from mpl_financa_modify import candlestick_ohlc

import pyspark.sql.functions as funsp
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.sql.window import Window
from colorama import Fore


matplotlib.use('Qt5Agg')
# Запускаем сессию pyspark.
spark = SparkSession.builder.getOrCreate()


class Drop:
    """ Класс создает объект из списков куда отмечаются файлы:
        для удаления и переименования временных файлов в постоянные. """
    def __init__(self):
        self.drop_array = []
        self.rename = []

    def app_drop(self, path, file):
        """ Метод помечает файл для удаления. """
        self.drop_array.append([path, file])

    def app_rename(self, old_name, new_name):
        """ Метод помечает файл для переименования. """
        self.rename.append([old_name, new_name])

    def drop_old(self):
        """ Метод удаляет все ранние созданные файлы, относящиеся к списку отмеченных файлов. """
        if self.drop_array:
            for x in self.drop_array:
                path_drop = x[0]
                der = x[1]
                drop_dir = os.listdir(path_drop)
                for drop_ in drop_dir:
                    if der[0:len(der) - 10:1] in drop_:
                        if der[len(der) - 10:len(der):1] in drop_:
                            continue
                        else:
                            for drop_file in os.listdir(f'{path_drop}{drop_}'):
                                os.remove(f'{path_drop}{drop_}/{drop_file}')
                            os.rmdir(f'{path_drop}{drop_}')
                            break
            self.drop_array.clear()

    def rename_dir(self):
        """ Метод переименовывает все файлы относящие к списку отмеченных файлов,
            переводя их из статуса временных в постоянные"""
        if self.rename:
            for x in self.rename:
                os.rename(x[0], x[1])
            self.rename.clear()


# Создаем объект в который будем собирать ссылки для удаления временных и устаревших файлов с данными.
data_old = Drop()


def get_new_file_tool(data_frame=None, f=None):
    """ Метод создаёт дата фрейм из данных загруженных с сервера,
        и определяет последнюю дату представленной информации. """
    # spark.sparkContext.addFile(url)
    df = (spark.read.format('csv')
          .option("header", 'true')
          .option("delimiter", ";")
          .option("inferSchema", "true")
          .option("comment", "h")
          .load(f)
          )
    if not (data_frame is None):
        df = DataFrame.union(df, data_frame)
    if df.select(funsp.count(funsp.when(funsp.col('WAPRICE').isNull(), 'WAPRICE'))).collect()[0][0] == 0:
        control_date = df.agg(funsp.max('TRADEDATE')).collect()[0][0]
    else:
        control_date = None
    return df, control_date


def get_frame_in_base(file_name):
    """ Метод поднимает данные с локального диска. """
    df = (spark.read.format("csv")
          .option("header", 'true')
          .option("delimiter", ";")
          .option("inferSchema", "true")
          .load(file_name))
    return df


def save_data(df, der):
    """ Метод сортирует и сохраняет данные по торговому инструменту. """
    df = df.distinct().orderBy('TRADEDATE')
    df = df.repartition(1)
    df.write.mode("overwrite").format("csv").option("header", "true") \
        .option("delimiter", ";").save(f'data/enter/{der}')
    data_old.app_drop('data/enter/', der)


def data_get(path_dir='.', file_data=None, schema=None, colons='shares'):
    """ Метод преобразует данные по торговому инструменту в записи для торговых таблиц. """

    if file_data is None:
        raise ValueError('The data loading path is not defined')

    date = apirestmoex.today(apirestmoex.yeas_sec)
    colons = config.colons[colons]

    if schema is None:
        df = (spark.read.format("csv")
              .option("header", 'true')
              .option("delimiter", ";")
              .option("inferSchema", "true")
              .load(f'{path_dir}{file_data}'))
    else:
        df = (spark.read.format("csv")
              .option("header", 'true')
              .option("delimiter", ";")
              .option("inferSchema", "true")
              .schema(schema)
              .load(f'{path_dir}{file_data}'))

    # Отсекаем лишние данные по дате.
    df = df.select(colons).distinct().filter(f'TRADEDATE > "{date}"').orderBy('TRADEDATE')

    up_data = True
    # Собираем данные для графика.
    graphic = df.select('TRADEDATE', 'SHORTNAME', 'SECID', 'OPEN', 'LOW', 'HIGH', 'CLOSE', )
    graphic.repartition(1).write.mode("overwrite").format("parquet").save(f'data/graphic/{file_data}')
    data_old.app_drop('data/graphic/', file_data)

    # Создаём шаблон строки для записи в торговую таблицу.
    actual_date = df.agg(funsp.max('TRADEDATE')).collect()[0][0]
    trade_table = df.select('SHORTNAME', 'SECID', 'BOARDID', funsp.col('WAPRICE').alias('PRICE')) \
        .where(f'TRADEDATE = "{actual_date}"')

    # Создаём колонку для таблицы расчета зависимости между торговыми инструментами.
    in_corr = df.select('TRADEDATE', funsp.col('WAPRICE').alias(df.distinct().select('SECID').collect()[0][0]))

    # Добавим расчётов.
    df = (df
          # Добавим среднюю цену между открытием и закрытием.
          .withColumn('temp_s', ((funsp.col('OPEN') + funsp.col('CLOSE')) / 2))
          # Узнаем направление движения актива.
          .withColumn('DYNAMIC', (funsp
                                  .when(funsp.col('OPEN') > funsp.col('CLOSE'), 'falling')
                                  .when(funsp.col('OPEN') == funsp.col('CLOSE'), 'stating')
                                  .otherwise('growing')))
          # Узнаем отклонение цены за день, приводя показания к процентам.
          .withColumn('D_VAR', (funsp
                                .when(funsp.col('DYNAMIC') == 'growing',
                                      (funsp.col('HIGH') - funsp.col('temp_s')) / funsp.col('WAPRICE'))
                                .when(funsp.col('DYNAMIC') == 'stating', 0)
                                .otherwise((funsp.col('temp_s') - funsp.col('LOW')) / funsp.col('WAPRICE'))))
          # Узнаем всё движение цены за день.
          .withColumn('VAL_DAY', (funsp.col('HIGH') - funsp.col('LOW')))
          # Узнаем средние объёмы сделок.
          .withColumn('MID_LOT', (funsp.col('VOLUME') / funsp.col('NUMTRADES')))
          # На этом этапе нам уже не нужны столбцы:
          .drop('OPEN').drop('LOW').drop('HIGH').drop('CLOSE')
          .drop('WAPRICE').drop('temp_s').drop('VOLUME').drop('NUMTRADES')
          )
    # Соотнесем показатели волатильности и размера среднего лота за год с дневными.
    avg_df = df.agg(funsp.avg('MID_LOT') * funsp.avg('VAL_DAY')).collect()[0][0]
    df = df.withColumn('LOT_VAL', ((funsp.col('MID_LOT') * funsp.col('VAL_DAY')) / avg_df)) \
        .drop('MID_LOT').drop('VAL_DAY')
    # Группируем тренды.
    w_id = Window.partitionBy('SECID').orderBy('TRADEDATE')
    df = (df
          .withColumn("is_last_row_in_window", funsp.lead("DYNAMIC", 1, True).over(w_id) != funsp.col("DYNAMIC"))
          .withColumn("window_id",
                      funsp.lag(funsp.sum(funsp.col("is_last_row_in_window").cast("int")).over(w_id), 1, 0).over(
                          w_id))
          # Движение тренда.
          .withColumn("sum_value", funsp.sum('D_VAR').over(Window.partitionBy("window_id")))
          # Берём медианное значение влияния волатильности и цены лота.
          .withColumn("maxL", funsp.max('LOT_VAL').over(Window.partitionBy("window_id")))
          .withColumn("minL", funsp.min('LOT_VAL').over(Window.partitionBy("window_id")))
          .where("is_last_row_in_window")
          .drop('D_VAR').drop('LOT_VAL').drop('is_last_row_in_window')
          )
    # Не получило сразу сформировать значение медианы, досчитаем отдельно.
    df = df.withColumn('median', ((funsp.col("maxL") + funsp.col("minL")) / 2)).drop('maxL').drop('minL')
    # Рассчитаем линейную зависимость между изменениями цены и объёмами торгов.
    cov_corr = df.stat.corr("median", "sum_value")
    # Ну и склеим всё как-нибудь в одно.
    df2 = (df.groupBy('DYNAMIC').agg((funsp.avg('sum_value') + (funsp.avg('sum_value') * funsp.avg("median")
                                                                * cov_corr)).alias('result'))
           .withColumn('GR_FAL', funsp.col('result')).drop('result')
           )
    # Добавим наши расчеты, ради которых все и затевалось, в строку для записи в торговую таблицу.
    try:
        trade_table = (trade_table
                       .withColumn('RISK_P', funsp.round(funsp.lit(df2.select('GR_FAL').where('DYNAMIC = "falling"')
                                                                   .collect()[0][0] * 100), 2))
                       .withColumn('PROFIT_P',
                                   funsp.round(funsp.lit(df2.select('GR_FAL').where('DYNAMIC = "growing"')
                                                         .collect()[0][0] * 100), 2)))
    except IndexError:
        up_data = False
    except TypeError:
        up_data = False
    return up_data, in_corr, trade_table


def graph_show(gr_file, date1='min', date2='max'):
    """ Метод строит график цен по данным из принятого файла. """
    if date1 == 'min':
        date1 = "0000-01-01"
    if date2 == 'max':
        date2 = "2999-12-01"

    mondays = WeekdayLocator(mpl_dates.MONDAY)
    all_days = DayLocator()
    week_formatter = DateFormatter('%Y %b %d')

    graph = (spark.read.format("parquet")
             .option("header", 'true')
             .option("delimiter", ";")
             .load(gr_file))
    graph = graph.filter(f'TRADEDATE > "{date1}" and TRADEDATE < "{date2}"').toPandas()
    title = graph['SHORTNAME'][0]

    fig = plt.figure(figsize=(16, 9))
    fig.set(facecolor='#EEFFBB')
    ax = fig.add_subplot()
    fig.suptitle(title, fontsize=24, c='m', fontstyle='italic', bbox={'boxstyle': 'round', 'facecolor': '#E1A0FF'})
    fig.subplots_adjust(bottom=0.2)
    ax.set(facecolor='#AAFFFF')
    ax.xaxis.set_major_locator(mondays)
    ax.xaxis.set_minor_locator(all_days)
    ax.xaxis.set_major_formatter(week_formatter)
    ax.set_ylabel('Цена, руб.', fontsize=14, rotation='horizontal', y=1, horizontalalignment='right',
                  bbox={'boxstyle': 'round', 'facecolor': 'c'})

    candlestick_ohlc(ax, zip(mpl_dates.date2num(graph['TRADEDATE']), graph['OPEN'], graph['HIGH'], graph['LOW'],
                             graph['CLOSE']), width=0.6, colorup='g', colordown='r')

    ax.xaxis_date()
    ax.autoscale_view()
    plt.setp(plt.gca().get_xticklabels(), rotation=75, horizontalalignment='right')
    plt.grid()
    plt.show()


class Trader:
    """ Класс создаёт дата фреймы для подбора торговой стратегии, если есть ранее записанные данные,
        то загружаем их, если нет создаём пустые дата фреймы. """
    def __init__(self):
        schema = StructType([StructField('SHORTNAME', StringType()),
                             StructField('SECID', StringType()),
                             StructField('BOARDID', StringType()),
                             StructField('PRICE', FloatType()),
                             StructField('RISK_P', FloatType()),
                             StructField('PROFIT_P', FloatType())
                             ])
        schema2 = StructType([StructField('TRADEDATE', StringType()),
                              StructField('SECID', FloatType())
                              ])
        dir_trade = os.listdir(f'data/data_for_spark')
        if 'in_corr__00-00-00' in dir_trade:
            self.df_corr = (spark.read.format("csv")
                            .option("header", 'true')
                            .option("delimiter", ";")
                            .option("inferSchema", "true")
                            .load('data/data_for_spark/in_corr__00-00-00'))
        else:
            self.df_corr = spark.createDataFrame([], schema2)
        if 'trade_table__00-00-00' in dir_trade:
            self.df_trade = (spark.read.format("csv")
                             .option("header", 'true')
                             .option("delimiter", ";")
                             .option("inferSchema", "true")
                             .schema(schema)
                             .load('data/data_for_spark/trade_table__00-00-00'))
        else:
            self.df_trade = spark.createDataFrame([], schema)

    def update_trade(self, in_corr, table_trade):
        """ Метод принимает обработанные данные по новому инструменту
            и добавляет их в торговые таблицы или обновляет. """

        drop_col = in_corr.columns[1 - in_corr.columns.index('TRADEDATE')]
        if drop_col in self.df_corr.columns:
            self.df_corr = self.df_corr.drop(drop_col)
        self.df_corr = self.df_corr.join(in_corr, 'TRADEDATE', 'outer')

        drop_row = table_trade.select('SECID').collect()[0][0]
        self.df_trade = self.df_trade.where(f'SECID != "{drop_row}"')
        self.df_trade = DataFrame.union(self.df_trade, table_trade)

    def write_data(self):
        """ Метод сохраняет торговые таблицы в файлы. """

        if 'SECID' in self.df_corr.columns:
            self.df_corr = self.df_corr.drop('SECID')
        self.df_corr.repartition(1).write.mode("overwrite").format("csv").option("header", "true") \
            .option("delimiter", ";").save('data/data_for_spark/in_corr__00-00-01')
        data_old.app_drop('data/data_for_spark/', 'in_corr__00-00-01')
        data_old.app_rename('data/data_for_spark/in_corr__00-00-01', 'data/data_for_spark/in_corr__00-00-00')

        self.df_trade.repartition(1).write.mode("overwrite").format("csv").option("header", "true") \
            .option("delimiter", ";").save('data/data_for_spark/trade_table__00-00-01')
        data_old.app_drop('data/data_for_spark/', 'trade_table__00-00-01')
        data_old.app_rename('data/data_for_spark/trade_table__00-00-01', 'data/data_for_spark/trade_table__00-00-00')

    def create_table_corr(self):
        """ Метод преобразует таблицу с ценами торговых инструментов
            в таблицу зависимостей торговых инструментов между собой. """

        self.df_corr = self.df_corr.drop('TRADEDATE')

        schem_col = self.df_corr.columns
        corr_array = list(map(lambda it: [], schem_col))
        for r_line in self.df_corr.collect():
            col_itr = 0
            for elm in r_line:
                if elm is None:
                    elm = 0
                corr_array[col_itr].append(float(elm))
                col_itr += 1

        corr_array = numpy.vstack(corr_array)
        corr_array = numpy.corrcoef(corr_array)
        print(Fore.CYAN + 'Почти готово ...' + Fore.GREEN)

        data_list = []
        col_itr = 0
        for itr in corr_array:
            data_list.append([])
            for elm in itr:
                data_list[col_itr].append(float(elm))
            data_list[col_itr].append(schem_col[col_itr])
            col_itr += 1

        new_schem = [StructField(itr, FloatType()) for itr in schem_col]
        new_schem.append(StructField('SECID', StringType()))
        rdd = spark.sparkContext.parallelize(data_list)
        self.df_corr = spark.createDataFrame(rdd, StructType(new_schem))

    def tc(self, cortege):
        """ Метод выводит предложение по инвестициям оптимальное для заданных параметров, но это не точно ... """

        en_risk = cortege[0]
        en_prof = cortege[1]
        en_sum = cortege[2]
        self.df_trade.createOrReplaceTempView("df_trade")
        self.df_corr.createOrReplaceTempView("df_corr")
        ds_one = spark.sql(f"""
            select SHORTNAME, SECID, BOARDID, PRICE from df_trade
            where PROFIT_P = (select max(PROFIT_P)
                              from df_trade
                              where RISK_P < {en_risk} and PRICE < {en_sum/4})
        """)
        secid = ds_one.select('SECID').collect()[0][0]
        if ds_one.agg(funsp.count('SECID')).collect()[0][0] > 0:
            for itr in range(10):
                ds_two = spark.sql(f"""
                    with rdf as (select df_trade.SECID, RISK_P
                                from df_trade
                                join df_corr using (SECID)
                                where {secid} < {config.corr} and PROFIT_P > {en_prof} and PRICE < {en_sum / 4})                              
                    select SHORTNAME, SECID, BOARDID, PRICE from rdf
                    join df_trade using (SECID)
                    where rdf.RISK_P = (select min(RISK_P) from rdf)                            
                """)
                if ds_two.agg(funsp.count('SECID')).collect()[0][0] > 0:
                    ds_tre = DataFrame.union(ds_one, ds_two)
                    ds_tre = DataFrame.union(ds_tre, spark.sql(f"""
                        with rdf as (select df_trade.SECID, PROFIT_P, RISK_P,
                                    case when RISK_P < {en_risk} then 'r'
                                    when  PROFIT_P > {en_prof} then 'p'                                              
                                    else 'd' end sel
                                    from df_trade                                    
                                    join df_corr using (SECID)
                                    where (({secid} < 0 and {secid} > -0.3)
                                    or ({secid} > 0 and {secid} < 0.3)) and PRICE < {en_sum / 4})                              
                            select SHORTNAME, SECID, BOARDID, PRICE
                            from rdf  
                            join df_trade using (SECID)
                            where rdf.PROFIT_P = (select max(PROFIT_P) from rdf where sel = 'r') 
                            or rdf.RISK_P = (select min(RISK_P) from rdf where sel = 'p')                                
                    """))
                    (ds_tre
                     .distinct()
                     .withColumn('Наименование', funsp.col('SHORTNAME')).drop('SHORTNAME')
                     .withColumn('Код актива', funsp.col('SECID')).drop('SECID')
                     .withColumn('Код режима торгов', funsp.col('BOARDID')).drop('BOARDID')
                     .withColumn('Цена', funsp.col('PRICE')).drop('PRICE')
                     .withColumn('Количество', funsp.round(en_sum / funsp.col('Цена') /
                                                           ds_tre.agg(funsp.count('PRICE')).collect()[0][0], 0)).show()
                     )
                    break
                en_prof *= 0.9
            else:
                print(Fore.LIGHTRED_EX, end='')
                print('К сожалению нет возможности сформировать предложение с указанными параметрами.')
                print(Fore.LIGHTBLUE_EX + 'Попробуйте уменьшить ожидаемую прибыль.' + Fore.GREEN)
        else:
            print(Fore.LIGHTRED_EX, end='')
            print('К сожалению нет возможности сформировать предложение с указанными параметрами.')
            print(Fore.LIGHTBLUE_EX + 'Может стоит больше рискнут?' + Fore.GREEN)

    def show_df(self):
        self.df_corr.show()
        self.df_trade.show()
        self.df_trade.agg(funsp.count('SECID')).show()

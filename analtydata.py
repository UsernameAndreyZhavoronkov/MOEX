import numpy
# from apirestmoex import yea_old

from pyspark.sql import SparkSession  # DataFrame
from pyspark.sql.types import StructField, StringType, StructType, DoubleType, IntegerType
import pyspark.sql.functions as funsp
from pyspark.sql.window import Window


def data_get(mas_data=None,
             schema=None,
             colons=None,
             date='2021-11-15'  # yea_old()
             ):
    if mas_data is None:
        return 'WARNING: The data loading path is not defined'
    if colons is None:
        colons = ['BOARDID',
                  'TRADEDATE',
                  'SHORTNAME',
                  'SECID',
                  'NUMTRADES',
                  'OPEN',
                  'LOW',
                  'HIGH',
                  'CLOSE',
                  'WAPRICE',
                  'VOLUME']
    spark = SparkSession.builder.getOrCreate()
    if schema is None:
        df = (spark.read.format("csv")
              .option("header", 'true')
              .option("delimiter", ";")
              .option("inferSchema", "true")
              .load(mas_data))
    else:
        df = (spark.read.format("csv")
              .option("header", 'true')
              .option("delimiter", ";")
              .option("inferSchema", "true")
              .schema(schema)
              .load(mas_data))

    # Отсекаем лишние данные по дате
    df = df.select(colons).distinct().filter(f'TRADEDATE > "{date}"').orderBy('TRADEDATE')
    # Собираем данные для графика
    graphic = df.select('TRADEDATE', 'SHORTNAME', 'SECID', 'OPEN', 'LOW', 'HIGH', 'CLOSE', )
    # Добавим расчётов
    df = (df
          # Добавим среднюю цену между открытием и закрытием
          .withColumn('temp_s', ((funsp.col('OPEN') + funsp.col('CLOSE')) / 2))
          # Узнаем направление движения актива
          .withColumn('DYNAMIC', (funsp
                                  .when(funsp.col('OPEN') > funsp.col('CLOSE'), 'falling')
                                  .when(funsp.col('OPEN') == funsp.col('CLOSE'), 'stating')
                                  .otherwise('growing')))
          # Узнаем отклонение цены за день, приводя показания к процентам
          .withColumn('DVAR', (funsp
                               .when(funsp.col('DYNAMIC') == 'growing',
                                     (funsp.col('HIGH') - funsp.col('temp_s')) / funsp.col('WAPRICE'))
                               .when(funsp.col('DYNAMIC') == 'stating', 0)
                               .otherwise((funsp.col('temp_s') - funsp.col('LOW')) / funsp.col('WAPRICE'))))
          # Узнаем всё движение цены за день
          .withColumn('VALDAY', (funsp.col('HIGH') - funsp.col('LOW')))
          # Узнаем средние объёмы сделок
          .withColumn('MIDLOT', (funsp.col('VOLUME') / funsp.col('NUMTRADES')))
          # На этом этапе нам уже не нужны столбцы:
          .drop('OPEN').drop('LOW').drop('HIGH').drop('CLOSE')
          .drop('WAPRICE').drop('temp_s').drop('VOLUME').drop('NUMTRADES')
          )
    # Соотнесем показатели волатильности и размера среднего лота за год с дневными
    avgdf = df.agg(funsp.avg('MIDLOT') * funsp.avg('VALDAY')).collect()[0][0]
    df = df.withColumn('LOTVAL', ((funsp.col('MIDLOT') * funsp.col('VALDAY')) / avgdf)).drop('MIDLOT').drop('VALDAY')
    # Группируем тренды
    w_id = Window.orderBy('TRADEDATE')
    df2 = (df
           .withColumn("is_last_row_in_window", funsp.lead("DYNAMIC", 1, True).over(w_id) != funsp.col("DYNAMIC"))
           .withColumn("window_id",
                       funsp.lag(funsp.sum(funsp.col("is_last_row_in_window").cast("int")).over(w_id), 1, 0).over(w_id))
           # Движение тренда
           .withColumn("sum_value", funsp.sum('DVAR').over(Window.partitionBy("window_id")))
           # Берём медианное значение влияния волатильности и цены лота
           .withColumn("median", ((funsp.max('LOTVAL') + funsp.min('LOTVAL'))/2).over(Window.partitionBy("window_id")))
           .where("is_last_row_in_window")
           )

    df2.show()

    df.show()

    #
    #
    # df.select('OPEN', 'CLOSE', 'DTAR').show()

    # uy = df.select('TRADEDATE').distinct()
    # df.select('SECID').distinct().show()
    # uy.agg(funsp.count('*')).show()

    # print(df.columns)
    # dfc.select('name', 'title').show()
    # dfc.select('name', 'title').filter(funsp.col('name').isin(df.columns)).show()
    # df.agg(funsp.max(funsp.to_date('TRADEDATE')).cast(StringType()))
    # out = df.select(funsp.col('CLOSE'), funsp.col('OPEN'))
    return ''


if __name__ == '__main__':
    c = []
    for i in range(3):
        i += 1
        c.append(f'data/enter/stock_shares_TQBR_SBER_0000{i}.csv')
    data_get(c)

    # x = []
    # VAL = numpy.vstack(x)
    # R_xy = numpy.corrcoef(x)
    # print(R_xy)

    print('end')

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from tpch.config.ConfigStore import *
from tpch.udfs.UDFs import *

def WindowFunction_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn(
          "sum_qty",
          sum(col("l_quantity"))\
            .over(Window.partitionBy().orderBy(col("l_returnflag").asc(), col("l_linestatus").asc()))
        )\
        .withColumn(
          "sum_base_price",
          sum(col("l_extendedprice"))\
            .over(Window.partitionBy().orderBy(col("l_returnflag").asc(), col("l_linestatus").asc()))
        )\
        .withColumn(
          "sum_disc_price",
          sum((col("l_extendedprice") * (lit(1) - col("l_discount"))))\
            .over(Window.partitionBy().orderBy(col("l_returnflag").asc(), col("l_linestatus").asc()))
        )\
        .withColumn(
          "sum_charge",
          sum(((col("l_extendedprice") * (lit(1) - col("l_discount"))) * (lit(1) + col("l_tax"))))\
            .over(Window.partitionBy().orderBy(col("l_returnflag").asc(), col("l_linestatus").asc()))
        )\
        .withColumn(
          "avg_qty",
          avg(col("l_extendedprice"))\
            .over(Window.partitionBy().orderBy(col("l_returnflag").asc(), col("l_linestatus").asc()))
        )\
        .withColumn(
          "avg_disc",
          avg(col("l_discount"))\
            .over(Window.partitionBy().orderBy(col("l_returnflag").asc(), col("l_linestatus").asc()))
        )\
        .withColumn("count_order", count(lit(1)).over(Window.partitionBy().orderBy(col("l_returnflag").asc(), col("l_linestatus").asc())))

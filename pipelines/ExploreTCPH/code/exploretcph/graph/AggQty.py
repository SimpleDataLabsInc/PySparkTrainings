from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from exploretcph.config.ConfigStore import *
from exploretcph.udfs.UDFs import *

def AggQty(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("L_RETURNFLAG"), col("L_LINESTATUS"))

    return df1.agg(
        sum(col("L_QUANTITY")).alias("SUM_QTY"), 
        sum(col("L_EXTENDEDPRICE")).alias("SUM_BASE_PRICE"), 
        sum(col("L_EXTENDEDPRICE") * lit(1) - col("L_DISCOUNT")).alias("SUM_DISC_PRICE"), 
        sum(col("L_EXTENDEDPRICE") * lit(1) - col("L_DISCOUNT") * lit(1) + col("L_TAX")).alias("SUM_CHARGE"), 
        avg(col("L_QUANTITY")).alias("AVG_QTY"), 
        avg(col("L_DISCOUNT")).alias("AVG_DISC"), 
        count(lit(1)).alias("count_order")
    )

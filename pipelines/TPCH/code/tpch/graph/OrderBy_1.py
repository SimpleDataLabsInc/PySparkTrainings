from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from tpch.config.ConfigStore import *
from tpch.udfs.UDFs import *

def OrderBy_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.orderBy(col("l_returnflag").asc(), col("l_linestatus").asc())

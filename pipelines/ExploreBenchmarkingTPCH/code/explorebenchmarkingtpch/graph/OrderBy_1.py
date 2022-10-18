from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from explorebenchmarkingtpch.config.ConfigStore import *
from explorebenchmarkingtpch.udfs.UDFs import *

def OrderBy_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.orderBy(col("L_RETURNFLAG").asc(), col("L_LINESTATUS").asc(), col("SUM_QTY").asc())

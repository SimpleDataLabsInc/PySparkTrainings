from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from test.config.ConfigStore import *
from test.udfs.UDFs import *

def OrderBy(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.orderBy(col("L_RETURNFLAG").asc(), col("L_LINESTATUS").asc())

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from exploretcph.config.ConfigStore import *
from exploretcph.udfs.UDFs import *

def SQLStatement_1(spark: SparkSession, LINEITEM: DataFrame) -> (DataFrame):
    LINEITEM.createOrReplaceTempView("LINEITEM")
    df1 = spark.sql("SELECT * FROM LINEITEM where (L_TAX is null);")

    return df1

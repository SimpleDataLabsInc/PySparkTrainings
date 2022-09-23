from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from tpch.config.ConfigStore import *
from tpch.udfs.UDFs import *

def TPCH_SF1_PARTSUPP(spark: SparkSession) -> DataFrame:
    from pyspark.dbutils import DBUtils

    return spark.read\
        .format("snowflake")\
        .options(
          **{
            "sfUrl": "https://HA30422.us-east-2.aws.snowflakecomputing.com",
            "sfUser": DBUtils(spark).secrets.get(scope = "anyademos", key = "username"),
            "sfPassword": DBUtils(spark).secrets.get(scope = "anyademos", key = "password"),
            "sfDatabase": "SNOWFLAKE_SAMPLE_DATA",
            "sfSchema": "TPCH_SF1",
            "sfWarehouse": ""
          }
        )\
        .option("dbtable", "PARTSUPP")\
        .load()

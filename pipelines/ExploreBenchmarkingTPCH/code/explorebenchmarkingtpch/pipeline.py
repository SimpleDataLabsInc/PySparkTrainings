from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from explorebenchmarkingtpch.config.ConfigStore import *
from explorebenchmarkingtpch.udfs.UDFs import *
from prophecy.utils import *
from explorebenchmarkingtpch.graph import *

def pipeline(spark: SparkSession) -> None:
    df_TPCH_SF1_LINEITEM = TPCH_SF1_LINEITEM(spark)
    df_Reformat_1 = Reformat_1(spark, df_TPCH_SF1_LINEITEM)
    df_Aggregate_1 = Aggregate_1(spark, df_Reformat_1)
    df_OrderBy_1 = OrderBy_1(spark, df_Aggregate_1)
    lineitem_aggregation(spark, df_OrderBy_1)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "3390/pipelines/ExploreBenchmarkingTPCH")
    MetricsCollector.start(spark = spark, pipelineId = "3390/pipelines/ExploreBenchmarkingTPCH")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()

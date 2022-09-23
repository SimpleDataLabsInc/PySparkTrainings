from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from exploretcph.config.ConfigStore import *
from exploretcph.udfs.UDFs import *
from prophecy.utils import *
from exploretcph.graph import *

def pipeline(spark: SparkSession) -> None:
    Target_1(spark)
    df_Source_0 = Source_0(spark)
    df_Reformat_1 = Reformat_1(spark)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "3328/pipelines/ExploreTCPH")
    MetricsCollector.start(spark = spark, pipelineId = "3328/pipelines/ExploreTCPH")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()

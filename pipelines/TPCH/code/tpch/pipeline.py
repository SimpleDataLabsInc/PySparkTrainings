from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from tpch.config.ConfigStore import *
from tpch.udfs.UDFs import *
from prophecy.utils import *
from tpch.graph import *

def pipeline(spark: SparkSession) -> None:
    df_partsupp = partsupp(spark)
    df_SUPPLIERS_SF_source = SUPPLIERS_SF_source(spark)
    df_JoinSuppliersPartsupp = JoinSuppliersPartsupp(spark, df_SUPPLIERS_SF_source, df_partsupp)
    df_CallCollections_out0, df_CallCollections_out1 = CallCollections(spark, df_JoinSuppliersPartsupp)
    df_Deduplicate = Deduplicate(spark, df_CallCollections_out1)
    df_SuppliersToCall = SuppliersToCall(spark, df_CallCollections_out0)
    df_tcph_lineitem = tcph_lineitem(spark)
    df_ReplaceSpaces = ReplaceSpaces(spark, df_tcph_lineitem)
    df_LINEITEMS_LIMIT_100K_1 = LINEITEMS_LIMIT_100K_1(spark)
    df_TPCH_SF1_PARTSUPP = TPCH_SF1_PARTSUPP(spark)
    df_Join_1 = Join_1(spark, df_LINEITEMS_LIMIT_100K_1, df_TPCH_SF1_PARTSUPP)
    df_Limit_1 = Limit_1(spark, df_tcph_lineitem)
    df_WindowFunction_1 = WindowFunction_1(spark, df_Limit_1)
    df_Supplier = Supplier(spark)
    LINEITEMS_LIMIT_100K(spark, df_WindowFunction_1)
    df_MarkClearanceItems = MarkClearanceItems(spark, df_ReplaceSpaces)
    df_PART = PART(spark)
    df_Aggregate_1 = Aggregate_1(spark, df_Limit_1)
    df_RejectRecords = RejectRecords(spark, df_Supplier)
    SUPPLIERS_SF(spark, df_RejectRecords)
    df_OrderBy_1 = OrderBy_1(spark, df_Aggregate_1)
    Target_1(spark, df_OrderBy_1)
    TPCH_call_collections(spark, df_SuppliersToCall)
    df_Filter_1 = Filter_1(spark, df_Join_1)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "3390/pipelines/TPCH")
    MetricsCollector.start(spark = spark, pipelineId = "3390/pipelines/TPCH")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()

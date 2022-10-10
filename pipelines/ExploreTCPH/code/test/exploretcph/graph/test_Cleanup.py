from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from exploretcph.graph.Cleanup import *
import exploretcph.config.ConfigStore as ConfigStore


class CleanupTest(BaseTestCase):

    def test_unit_test_0(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/exploretcph/graph/Cleanup/in0/schema.json',
            'test/resources/data/exploretcph/graph/Cleanup/in0/data/test_unit_test_0.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/exploretcph/graph/Cleanup/out/schema.json',
            'test/resources/data/exploretcph/graph/Cleanup/out/data/test_unit_test_0.json',
            'out'
        )
        dfOutComputed = Cleanup(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select(
              "L_ORDERKEY",
              "L_PARTKEY",
              "L_SUPPKEY",
              "L_LINENUMBER",
              "L_QUANTITY",
              "L_EXTENDEDPRICE",
              "L_DISCOUNT",
              "L_TAX",
              "L_RETURNFLAG",
              "L_LINESTATUS",
              "L_SHIPDATE",
              "L_COMMITDATE",
              "L_RECEIPTDATE",
              "L_SHIPINSTRUCT",
              "L_SHIPMODE",
              "L_COMMENT",
              "L_CLEARANCE"
            ),
            dfOutComputed.select(
              "L_ORDERKEY",
              "L_PARTKEY",
              "L_SUPPKEY",
              "L_LINENUMBER",
              "L_QUANTITY",
              "L_EXTENDEDPRICE",
              "L_DISCOUNT",
              "L_TAX",
              "L_RETURNFLAG",
              "L_LINESTATUS",
              "L_SHIPDATE",
              "L_COMMITDATE",
              "L_RECEIPTDATE",
              "L_SHIPINSTRUCT",
              "L_SHIPMODE",
              "L_COMMENT",
              "L_CLEARANCE"
            ),
            self.maxUnequalRowsToShow
        )

    def setUp(self):
        BaseTestCase.setUp(self)
        import os
        fabricName = os.environ['FABRIC_NAME']
        ConfigStore.Utils.initializeFromArgs(
            self.spark,
            Namespace(file = f"configs/resources/config/{fabricName}.json", config = None)
        )

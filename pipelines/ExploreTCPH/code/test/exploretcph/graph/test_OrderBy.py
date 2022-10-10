from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from exploretcph.graph.OrderBy import *
import exploretcph.config.ConfigStore as ConfigStore


class OrderByTest(BaseTestCase):

    def test_unit_test_0(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/exploretcph/graph/OrderBy/in0/schema.json',
            'test/resources/data/exploretcph/graph/OrderBy/in0/data/test_unit_test_0.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/exploretcph/graph/OrderBy/out/schema.json',
            'test/resources/data/exploretcph/graph/OrderBy/out/data/test_unit_test_0.json',
            'out'
        )
        dfOutComputed = OrderBy(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select(
              "L_RETURNFLAG",
              "L_LINESTATUS",
              "SUM_QTY",
              "SUM_BASE_PRICE",
              "SUM_DISC_PRICE",
              "SUM_CHARGE",
              "AVG_QTY",
              "AVG_DISC",
              "count_order"
            ),
            dfOutComputed.select(
              "L_RETURNFLAG",
              "L_LINESTATUS",
              "SUM_QTY",
              "SUM_BASE_PRICE",
              "SUM_DISC_PRICE",
              "SUM_CHARGE",
              "AVG_QTY",
              "AVG_DISC",
              "count_order"
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

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from exploretcph.graph.AggQty import *
import exploretcph.config.ConfigStore as ConfigStore


class AggQtyTest(BaseTestCase):

    def test_unit_test_0(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/exploretcph/graph/AggQty/in0/schema.json',
            'test/resources/data/exploretcph/graph/AggQty/in0/data/test_unit_test_0.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/exploretcph/graph/AggQty/out/schema.json',
            'test/resources/data/exploretcph/graph/AggQty/out/data/test_unit_test_0.json',
            'out'
        )
        dfOutComputed = AggQty(self.spark, dfIn0)
        assertDFEquals(dfOut.select("count_order"), dfOutComputed.select("count_order"), self.maxUnequalRowsToShow)

    def setUp(self):
        BaseTestCase.setUp(self)
        import os
        fabricName = os.environ['FABRIC_NAME']
        ConfigStore.Utils.initializeFromArgs(
            self.spark,
            Namespace(file = f"configs/resources/config/{fabricName}.json", config = None)
        )

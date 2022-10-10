import unittest

from test.exploretcph.graph.test_OrderBy import *
from test.exploretcph.graph.test_AggQty import *
from test.exploretcph.graph.test_Cleanup import *

if __name__ == "__main__":
    runner = unittest.TextTestRunner()
    runner.run(unittest.TestSuite())

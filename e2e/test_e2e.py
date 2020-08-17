import unittest
import hadar as hd


class E2ETest(unittest.TestCase):
    def test(self):
        # Input
        study = hd.Study(horizon=1)\
            .network()\
                .node('a')\
                    .consumption(name='load', cost=100, quantity=10)\
                    .production(name='prod', cost=10, quantity=10)\
            .build()

        optim = hd.RemoteOptimizer(url='http://localhost:8765')
        res = optim.solve(study)
        print(res)
        self.assertIsNotNone(res)
import unittest

from .basetest import BaseTest

class ReloadTest(BaseTest):
    def test_reload(self):
        self.geoserver.reload()


if __name__ == "__main__":
    unittest.main()


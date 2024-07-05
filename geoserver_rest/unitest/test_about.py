import unittest

from .basetest import BaseTest

class AboutTest(BaseTest):
    def test_get_version(self):
        for component in ["Geoserver","GeoTools","GeoWebCache"]:
            version = self.geoserver.get_version(component) 
            self.assertTrue(version is not None,"Failed to get the version of geoserver({})".format(self.geoserver.geoserver_url))
            print("The version of {1} running in the geoserver({0}) is {2}".format(self.geoserver.geoserver_url,component,".".join(str(v) for v in version)))


if __name__ == "__main__":
    unittest.main()


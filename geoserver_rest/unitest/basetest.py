import unittest
import os

from ..geoserver import Geoserver

class BaseTest(unittest.TestCase):
    _instance = None
    """
    BBOX (mixx,miny,maxx,maxy) or (left bottom coord, right top coord)
    """
    BBOX_AUSTRALIA = (113.338953078, -43.6345972634, 153.569469029, -10.6681857235)
    BBOX_WA = (112.839347,-35.180249,129,-13.713391)

    @classmethod
    def instance(cls):
        if cls._instance is None:
            cls._instance = cls()

        return cls._instance


    @classmethod
    def setUpClass(cls):
        print("=============================Begin to run unittest suit {}==========================================".format(cls.__name__))
        super().setUpClass()
        cls.geoserver = Geoserver(os.environ.get("GEOSERVER_URL"),os.environ.get("GEOSERVER_USER"),os.environ.get("GEOSERVER_PASSWORD"))

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
        print("=============================End to run unittest suit {}==========================================".format(cls.__name__))

    def setUp(self):
        print("****************************Begin to run test******************************************************")
        super().setUp()

    def tearDown(self):
        super().tearDown()
        print("****************************End to run test******************************************************")


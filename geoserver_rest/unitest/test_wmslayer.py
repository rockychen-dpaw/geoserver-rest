import unittest
import os
from .. import utils

from .basetest import BaseTest

class WMSLayerTest(BaseTest):
    def test_layername(self):
        test_workspace = "testws4unitest"
        #create the test workspace if doesn't have
        if self.geoserver.has_workspace(test_workspace):
            print("The testing workspace({}) already exist, delete it".format(test_workspace))
            self.geoserver.delete_workspace(test_workspace,True)

        print("Create the testing workspace({}) for testing".format(test_workspace))
        self.geoserver.create_workspace(test_workspace)
        recurse = True
        try:
            test_storename = "teststore4unitest"
            store_parameters = {
                "capabilitiesURL": os.environ.get("WMSSERVER_CAPABILITIESURL"),
                "user": os.environ.get("WMSSERVER_USER",None),
                "password": os.environ.get("WMSSERVER_PASSWORD",None),
                "maxConnections": 10,
                "readTimeout": 300,
                "connectTimeout": 60
            }
            print("Try to create the wmsstore({}) for testing".format(test_storename))
            self.assertTrue(self.geoserver.update_wmsstore(test_workspace,test_storename,store_parameters,create=True),"The wmsstore({}) should not exist before".format(test_storename))
            self.assertTrue(self.geoserver.has_wmsstore(test_workspace,test_storename),"The wmsstore({}) should be already created".format(test_storename))
            print("Create the wmsstore({}) successfully".format(test_storename))

            test_layername = "testlayer4unitest"
            parameters = {
                "title": "For unitesting",
                "nativeName": os.environ["WMSLAYER_NAME"]
            }
            print("Try to create the wmslayer({})".format(test_layername))
            self.assertTrue(self.geoserver.update_wmslayer(test_workspace,test_storename,test_layername,parameters,True),"The wmslayer({}) should not exist before".format(test_layername))
            self.assertTrue(self.geoserver.has_wmslayer(test_workspace,test_layername,storename=test_storename),"The wmslayer({}) should have been created".format(test_layername))
            self.assertTrue(self.geoserver.has_wmslayer(test_workspace,test_layername),"The wmslayer({}) should have been created".format(test_layername))
            self.assertEqual(len(self.geoserver.list_wmslayers(test_workspace,test_storename)),1,"The wmsstore({}) should have only one layer".format(test_storename))
            print("Create the wmslayer({}) successfully".format(test_layername))

            print("Try to update the wmslayer({})".format(test_layername))
            test_layerdata = self.geoserver.get_wmslayer(test_workspace,test_layername,storename=test_storename)
            parameters["srs"] = test_layerdata["srs"]
            parameters["nativeCRS"] = test_layerdata.get("nativeCRS")
            parameters["nativeBoundingBox"] = [test_layerdata["nativeBoundingBox"]["minx"],test_layerdata["nativeBoundingBox"]["miny"],test_layerdata["nativeBoundingBox"]["maxx"],test_layerdata["nativeBoundingBox"]["maxy"],test_layerdata["nativeBoundingBox"].get("crs","EPSG:4326")]
            parameters["latLonBoundingBox"] = [test_layerdata["latLonBoundingBox"]["minx"],test_layerdata["latLonBoundingBox"]["miny"],test_layerdata["latLonBoundingBox"]["maxx"],test_layerdata["latLonBoundingBox"]["maxy"],test_layerdata["latLonBoundingBox"].get("crs","EPSG:4326")]


            self.assertFalse(self.geoserver.update_wmslayer(test_workspace,test_storename,test_layername,parameters,False),"The wmslayer({}) should exist before".format(test_layername))
            test_layer = self.geoserver.get_wmslayer(test_workspace,test_layername,storename=test_storename)
            self.assertEqual(test_layer.get("nativeBoundingBox"),test_layerdata.get("nativeBoundingBox"),"The nativeBoundingBox of the wmslayer({}) should be {} instead of {}".format(test_layername,test_layerdata.get("nativeBoundingBox"),test_layer.get("nativeBoundingBox")) )
            self.assertEqual(test_layer.get("latLonBoundingBox"),test_layerdata.get("latLonBoundingBox"),"The latLonBoundingBox of the wmslayer({}) should be {} instead of {}".format(test_layername,test_layerdata.get("latLonBoundingBox"),test_layer.get("latLonBoundingBox")) )
            print("Update the wmslayer({}) successfully".format(test_layername))


            print("Try to update the wmslayer({})".format(test_layername))
            parameters["title"] = "Layer for unitesting".format(test_layername)
            self.assertFalse(self.geoserver.update_wmslayer(test_workspace,test_storename,test_layername,parameters,False),"The wmslayer({}) should exist before".format(test_layername))
            test_layer = self.geoserver.get_wmslayer(test_workspace,test_layername,storename=test_storename)
            self.assertEqual(test_layer.get("title"),parameters["title"],"The title of the wmslayer({}) should be {} instead of {}".format(test_layername,parameters["title"],test_layer.get("title")) )
            print("Update the wmslayer({}) successfully".format(test_layername))


            print("Try to update the wmslayer({})".format(test_layername))
            parameters["keywords"] = ["unitesting","by_rocky","geoserver_rest"]
            self.assertFalse(self.geoserver.update_wmslayer(test_workspace,test_storename,test_layername,parameters),"The wmslayer({}) should exist before".format(test_layername))
            test_layer = self.geoserver.get_wmslayer(test_workspace,test_layername,storename=test_storename)
            self.assertTrue(utils.is_contain(test_layer.get("keywords").get("string"),parameters["keywords"]),"The title of the wmslayer({}) should be {} instead of {}".format(test_layername,parameters["keywords"],test_layer.get("keywords")) )
            print("Update the wmslayer({}) successfully".format(test_layername))



            print("Try to delete the wmslayer({})".format(test_layername))
            self.assertTrue(self.geoserver.delete_wmslayer(test_workspace,test_layername,recurse=True),"The wmslayer({}) should exist before".format(test_layername))
            self.assertFalse(self.geoserver.delete_wmslayer(test_workspace,test_layername),"The wmslayer({}) should have been deleted before".format(test_layername))
            self.assertFalse(self.geoserver.has_wmslayer(test_workspace,test_layername,storename=test_storename),"The wmslayer({}) should have been deleted".format(test_layername))
            self.assertEqual(len(self.geoserver.list_wmslayers(test_workspace,test_storename)),0,"The wmsstore({}) should have no layer".format(test_storename))
            print("Delete the wmslayer({}) successfully".format(test_layername))

            print("Try to delete the wmsstore({})".format(test_storename))
            self.geoserver.delete_wmsstore(test_workspace,test_storename)
            self.assertFalse(self.geoserver.has_wmsstore(test_workspace,test_storename),"The wmsstore({}) should have been deleted".format(test_storename))
            print("Delete the wmsstore({}) successfully".format(test_storename))
            recurse = False
        finally:
            #delete the test workspace
            print("Delete the testing workspace({})".format(test_workspace))
            self.geoserver.delete_workspace(test_workspace,recurse=recurse)




if __name__ == "__main__":
    unittest.main()


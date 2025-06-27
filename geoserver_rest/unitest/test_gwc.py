import unittest
import os
from .. import utils
from datetime import datetime,timedelta

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
            print("Try to create the wmslayer({}) for testing".format(test_layername))
            self.assertTrue(self.geoserver.update_wmslayer(test_workspace,test_storename,test_layername,parameters,True),"The wmslayer({}) should not exist before".format(test_layername))
            self.assertTrue(self.geoserver.has_wmslayer(test_workspace,test_layername,storename=test_storename),"The wmslayer({}) should have been created".format(test_layername))
            print("Create the wmslayer({}) successfully".format(test_layername))

            parameters = {
                "mimeFormats" : ["image/png"],
                "gridSubsets" : [
                    {"name":"gda94"},
                    {"name":"mercator"}
                ],
                "metaWidth":1,
                "metaHeight":1,
                "expireCache":300,
                "expireClients":300,
                "enabled":True
            }
            print("Try to create the gwc cache layer for layer({})".format(test_layername))
            self.geoserver.update_gwclayer(test_workspace,test_layername,parameters)
            self.assertTrue(self.geoserver.has_gwclayer(test_workspace,test_layername),"The gwc cache layer for the layer({}) should have been created".format(test_layername))
            layerdata = self.geoserver.get_gwclayer(test_workspace,test_layername)
            for field in ("metaWidth","metaHeight","expireCache","expireClients","enabled"):
                self.assertEqual(str(self.geoserver.get_gwclayer_field(layerdata,field)),str(parameters[field]),"The field({2}) of the gwc layer ({0}:{1}) should should be {3} instead of {4}".format(test_workspace,test_layername,field,str(parameters[field]),str(self.geoserver.get_gwclayer_field(layerdata,field))))

            test_gwclayer = self.geoserver.get_gwclayer(test_workspace,test_layername)
            self.assertEqual(test_gwclayer["mimeFormats"],parameters["mimeFormats"],"The mimeFormat of the gwc cache layer for the layer({}) should have be {} instead of {}".format(test_layername,parameters["mimeFormats"],test_gwclayer["mimeFormats"]))
            self.assertEqual(len(self.geoserver.list_gwclayers(test_workspace)),1,"The workspace({}) should only contain one gwc cache layer".format(test_workspace))
            print("Create the gwc cache layer for layer({}) successfully".format(test_layername))

            print("Try to update the gwc cache layer for layer({})".format(test_layername))
            parameters["gridSubsets"][0]["extent"] = self.BBOX_AUSTRALIA
            parameters["mimeFormats"] = ["image/png","image/jpeg"]
            parameters["metaWidth"] = 10
            parameters["metaHeight"] = 10
            parameters["expireCache"] = 600
            self.geoserver.update_gwclayer(test_workspace,test_layername,parameters)
            test_gwclayer = self.geoserver.get_gwclayer(test_workspace,test_layername)
            test_gridset = next(d for d in test_gwclayer["gridSubsets"] if d["gridSetName"] == parameters["gridSubsets"][0]["name"])
            self.assertTrue(self.geoserver.has_gwclayer(test_workspace,test_layername),"The gwc cache layer for the layer({}) should have been created".format(test_layername))
            self.assertTrue(utils.has_samedata(test_gwclayer["mimeFormats"],parameters["mimeFormats"]),"The mimeFormats of the gwc cache layer for the layer({}) should have the value ({}) instead of {} ".format(test_layername,parameters["mimeFormats"],test_gwclayer["mimeFormats"]))
            self.assertEqual(list(test_gridset["extent"]["coords"]),list(parameters["gridSubsets"][0]["extent"]),"The gridset extent of the gwc cache layer for the layer({}) should have the value ({}) instead of {} ".format(test_layername,parameters["gridSubsets"][0]["extent"],test_gridset["extent"]["coords"]))
            layerdata = self.geoserver.get_gwclayer(test_workspace,test_layername)
            for field in ("metaWidth","metaHeight","expireCache","expireClients","enabled"):
                self.assertEqual(str(self.geoserver.get_gwclayer_field(layerdata,field)),str(parameters[field]),"The field({2}) of the gwc layer ({0}:{1}) should should be {3} instead of {4}".format(test_workspace,test_layername,field,str(parameters[field]),str(self.geoserver.get_gwclayer_field(layerdata,field))))

            print("Update the gwc cache layer for layer({}) successfully".format(test_layername))


            print("Try to empty the gwc cache layer for layer({})".format(test_layername))
            self.geoserver.empty_gwclayer(test_workspace,test_layername)

            
            print("Try to delete the gwc cache layer for layer({})".format(test_layername))
            self.geoserver.delete_gwclayer(test_workspace,test_layername)
            self.assertFalse(self.geoserver.has_gwclayer(test_workspace,test_layername),"The gwc cache layer for the layer({}) should have been deleted".format(test_layername))
            self.assertEqual(len(self.geoserver.list_gwclayers(test_workspace)),0,"The workspace({}) should have no gwc cache layer".format(test_workspace))
            print("Delete the gwc cache layer for layer({}) successfully".format(test_layername))

            print("Try to delete the wmslayer({})".format(test_layername))
            self.geoserver.delete_wmslayer(test_workspace,test_layername,recurse=True)
            self.assertFalse(self.geoserver.delete_wmslayer(test_workspace,test_layername),"The wmslayer({}) should have been deleted before".format(test_layername))
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


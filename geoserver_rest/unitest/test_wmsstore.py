import unittest
import os

from .basetest import BaseTest

class WMSStoreTest(BaseTest):
    def test_wmsstore(self):
        test_workspace = "testws4unitest"
        #create the test workspace if doesn't have
        if self.geoserver.has_workspace(test_workspace):
            print("The testing workspace({}) already exist, delete it".format(test_workspace))
            self.geoserver.delete_workspace(test_workspace,True)

        print("Create the testing workspace({}) for testing".format(test_workspace))
        self.geoserver.create_workspace(test_workspace)
        recurse = True
        try:
            storename = "teststore4unitest"
            parameters = {
                "capabilitiesURL": os.environ.get("WMSSERVER_CAPABILITIESURL"),
                "user": os.environ.get("WMSSERVER_USER",None),
                "password": os.environ.get("WMSSERVER_PASSWORD",None),
                "maxConnections": 10,
                "readTimeout": 300,
                "connectTimeout": 60
            }
            print("Try to create the wmsstore({})".format(storename))
            self.assertTrue(self.geoserver.update_wmsstore(test_workspace,storename,parameters,create=True),"The wmsstore({}) should be newly created".format(storename))
            self.assertTrue(self.geoserver.has_wmsstore(test_workspace,storename),"The wmsstore({}) should be already created".format(storename))
            print("Create the wmsstore({}) successfully".format(storename))

            parameters["maxConnections"] = 20
            parameters["connectTimeout"] = 30
            print("Try to update the wmsstore({})".format(storename))
            self.assertFalse(self.geoserver.update_wmsstore(test_workspace,storename,parameters,create=False),"The wmsstore({}) should be updated".format(storename))
            print("Update the wmsstore({}) successfully".format(storename))

            parameters["maxConnections"] = 10
            parameters["connectTimeout"] = 60
            print("Try to update the wmsstore({}) again".format(storename))
            self.assertFalse(self.geoserver.update_wmsstore(test_workspace,storename,parameters),"The wmsstore({}) should be updated".format(storename))
            print("Update the wmsstore({}) successfully".format(storename))

            print("list the wmsstores in workspace({})".format(test_workspace))
            self.assertEqual(len(self.geoserver.list_wmsstores(test_workspace)),1,"The workspace({}) should only have one wmsstore".format(test_workspace))

            print("Try to delete the wmsstore({})".format(storename))
            self.geoserver.delete_wmsstore(test_workspace,storename)
            self.assertFalse(self.geoserver.has_wmsstore(test_workspace,storename),"The wmsstore({}) should have been deleted".format(storename))
            print("Delete the wmsstore({}) successfully".format(storename))

            print("list the wmsstores in workspace({})".format(test_workspace))
            self.assertEqual(len(self.geoserver.list_wmsstores(test_workspace)),0,"The workspace({}) should have no wmsstore".format(test_workspace))

            recurse = False
        finally:
            #delete the test workspace
            print("Delete the testing workspace({})".format(test_workspace))
            self.geoserver.delete_workspace(test_workspace,recurse=recurse)




if __name__ == "__main__":
    unittest.main()


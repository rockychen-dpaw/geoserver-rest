import unittest
import os

from .basetest import BaseTest

class DatastoreTest(BaseTest):
    def test_datastore(self):
        test_workspace = "testws4unitest"
        #create the test workspace if doesn't have
        if self.geoserver.has_workspace(test_workspace):
            print("The testing workspace({}) already exist, delete it".format(test_workspace))
            self.geoserver.delete_workspace(test_workspace,True)

        print("Create the testing workspace({}) for testing".format(test_workspace))
        self.geoserver.create_workspace(test_workspace)
        recurse = True
        try:
            storename = "testds4unitest"
            parameters = {
                "host": os.environ.get("POSTGIS_HOST"),
                "port": os.environ.get("POSTGIS_PORT"),
                "database": os.environ.get("POSTGIS_DATABASE"),
                "schema": os.environ.get("POSTGIS_SCHEMA"),
                "user": os.environ.get("POSTGIS_USER"),
                "passwd": os.environ.get("POSTGIS_PASSWORD"),
                "Connection timeout": 5,
                "Max connection idle time": 600,
                "min connections": 5,
                "max connections": 20,
                "fetch size": 500
            }
            print("Try to create the datastore({})".format(storename))
            self.geoserver.update_datastore(test_workspace,storename,parameters,create=True)
            self.assertTrue(self.geoserver.has_datastore(test_workspace,storename),"The datastore({}) should be already created".format(storename))
            print("Create the datastore({}) successfully".format(storename))

            parameters["fetch size"] = 1000
            parameters["max connections"] = 30
            print("Try to update the datastore({})".format(storename))
            self.geoserver.update_datastore(test_workspace,storename,parameters,create=False)

            parameters["fetch size"] = 500
            parameters["max connections"] = 25
            print("Try to update the datastore({}) again".format(storename))
            self.geoserver.update_datastore(test_workspace,storename,parameters)

            print("list the datastores in workspace({})".format(test_workspace))
            self.assertEqual(len(self.geoserver.list_datastores(test_workspace)),1,"The workspace({}) should only have one datastore".format(test_workspace))

            print("Try to delete the datastore({})".format(storename))
            self.geoserver.delete_datastore(test_workspace,storename)
            self.assertFalse(self.geoserver.has_datastore(test_workspace,storename),"The datastore({}) should have been deleted".format(storename))
            print("Delete the datastore({}) successfully".format(storename))

            print("list the datastores in workspace({})".format(test_workspace))
            self.assertEqual(len(self.geoserver.list_datastores(test_workspace)),0,"The workspace({}) should have no datastore".format(test_workspace))

            recurse = False
        finally:
            #delete the test workspace
            print("Delete the testing workspace({})".format(test_workspace))
            self.geoserver.delete_workspace(test_workspace,recurse=recurse)




if __name__ == "__main__":
    unittest.main()


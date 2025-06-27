import unittest
import os

from .basetest import BaseTest
from .. import settings

class DatastoreTest(BaseTest):
    def test_postgis_datastore(self):
        if any(False if os.environ.get(key) else True for key in ("POSTGIS_HOST","POSTGIS_PORT","POSTGIS_DATABASE")):
            #test postgis datastore disabled
            print("Test postgis datastore is disabled.")
            return

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
            storedata = self.geoserver.get_datastore(test_workspace,storename)
            for k,v in parameters.items():
                if k == "passwd":
                    continue
                self.assertEqual(self.geoserver.get_datastore_field(storedata,k),str(v),"The field({1}) of the datastore({0}) should be {3} instead of {2}".format(storename,k,v,self.geoserver.get_datastore_field(storedata,k)))
            print("Create the datastore({}) successfully".format(storename))

            parameters["fetch size"] = 1000
            parameters["max connections"] = 30
            print("Try to update the datastore({})".format(storename))
            self.geoserver.update_datastore(test_workspace,storename,parameters,create=False)
            storedata = self.geoserver.get_datastore(test_workspace,storename)
            for k,v in parameters.items():
                if k == "passwd":
                    continue
                self.assertEqual(self.geoserver.get_datastore_field(storedata,k),str(v),"The field({1}) of the datastore({0}) should be {3} instead of {2}".format(storename,k,v,self.geoserver.get_datastore_field(storedata,k)))

            parameters["fetch size"] = 500
            parameters["max connections"] = 25
            print("Try to update the datastore({}) again".format(storename))
            self.geoserver.update_datastore(test_workspace,storename,parameters)
            storedata = self.geoserver.get_datastore(test_workspace,storename)
            for k,v in parameters.items():
                if k == "passwd":
                    continue
                self.assertEqual(self.geoserver.get_datastore_field(storedata,k),str(v),"The field({1}) of the datastore({0}) should be {3} instead of {2}".format(storename,k,v,self.geoserver.get_datastore_field(storedata,k)))

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


    def test_upload_dataset(self):
        sample_dataset = os.environ.get("SAMPLE_DATASET")
        if not sample_dataset or not os.path.exists(sample_dataset):
            print("can't find the sample data, test upload dataset is disabled")
            return

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
            print("Try to upload the dataset({})".format(sample_dataset))
            self.geoserver.upload_dataset(test_workspace,storename,sample_dataset)
            self.assertTrue(self.geoserver.has_datastore(test_workspace,storename),"The datastore({}) should be already created".format(storename))
            print("Create the datastore({}) successfully".format(storename))

            print("list the datastores in workspace({})".format(test_workspace))
            self.assertEqual(len(self.geoserver.list_datastores(test_workspace)),1,"The workspace({}) should only have one datastore".format(test_workspace))

            #print("Try to delete the datastore({})".format(storename))
            #self.geoserver.delete_datastore(test_workspace,storename)
            #self.assertFalse(self.geoserver.has_datastore(test_workspace,storename),"The datastore({}) should have been deleted".format(storename))
            #print("Delete the datastore({}) successfully".format(storename))

            #print("list the datastores in workspace({})".format(test_workspace))
            #self.assertEqual(len(self.geoserver.list_datastores(test_workspace)),0,"The workspace({}) should have no datastore".format(test_workspace))

        finally:
            #delete the test workspace
            #print("Delete the testing workspace({})".format(test_workspace))
            #self.geoserver.delete_workspace(test_workspace,recurse=recurse)
            pass


if __name__ == "__main__":
    unittest.main()


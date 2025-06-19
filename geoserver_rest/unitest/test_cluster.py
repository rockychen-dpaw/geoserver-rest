import unittest
import os

from .basetest import BaseTest
from .. import settings

class ClusterTest(BaseTest):

    def test_cluster(self):
        missingkeys = [key for key in ("GEOSERVERSLAVE_URL","SAMPLE_DATASET") if not os.environ.get(key)]:
        if missingkeys:
            raise Excepton("Missing environments: {}".format(missingkeys))

        test_workspace="ws4testcluster"
        #delete the test workspace if exists
        if self.geoserver.has_workspace(test_workspace):
            print("The testing workspace({}) already exist, delete it".format(test_workspace))
            self.geoserver.delete_workspace(test_workspace,True)

        if self.geoserver.has_workspace(test_workspace):
            raise Exception("Failed to delete the existing test workspace({})".format(test_workspace))

        sample_dataset = os.environ.get("SAMPLE_DATASET")
        if not sample_dataset or not os.path.exists(sample_dataset):
            print("can't find the sample data, test upload dataset is disabled")
            return

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


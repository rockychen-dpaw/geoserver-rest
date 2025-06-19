import unittest
import os
import json

from .basetest import BaseTest
from .. import settings

class LayergroupTest(BaseTest):

    def test_layergroup(self):
        test_workspace = "testws4unitest"
        #create the test workspace if doesn't have
        if self.geoserver.has_workspace(test_workspace):
            print("The testing workspace({}) already exist, delete it".format(test_workspace))
            self.geoserver.delete_workspace(test_workspace,True)

        print("Create the testing workspace({}) for testing".format(test_workspace))
        self.geoserver.create_workspace(test_workspace)
        recurse = True
        try:
            test_storename = "testds4unitest"
            store_parameters = {
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
            print("Try to create the datastore({}) for testing".format(test_storename))
            self.geoserver.update_datastore(test_workspace,test_storename,store_parameters,create=True)
            self.assertTrue(self.geoserver.has_datastore(test_workspace,test_storename),"The datastore({}) should be already created".format(test_storename))
            print("Create the datastore({}) successfully".format(test_storename))

            for test_layername,parameters in [(
                "testft4unitest",
                {
                    "title":"test feature type",
                    "abstract":"for unitest",
                    "keywords":["unitest"],
                    "srs":"EPSG:4326",
                    "nativeName":os.environ["POSTGIS_TABLE"]
                }
            ),(
                "testview4unitest",
                {
                    "title":"test feature type",
                    "abstract":"for unitest",
                    "keywords":["unitest"],
                    "srs":"EPSG:4326",
                    "viewsql":"select * from {}".format(os.environ["POSTGIS_TABLE"]),
                    "geometry_column":os.environ["POSTGIS_GEOMETRY_COLUMN"],
                    "geometry_type":os.environ["POSTGIS_GEOMETRY_TYPE"]
                }
            )]:
                print("Try to create the testing feature type({})".format(test_layername))
                self.geoserver.publish_featuretype(test_workspace,test_storename,test_layername,parameters,create=True)
                self.assertTrue(self.geoserver.has_featuretype(test_workspace,test_layername,storename=test_storename),"The layer({}:{}) should have been created".format(test_workspace,test_layername))

            test_groupname = "testgs4unitest"
            parameters = {
                "title": "test layergroup for unitest",
                "abstract": "test layergroup for unitest",
                "keywords": ["unitest","layergroup"],
                "layers":[
                    {"type":"layer","name":"testft4unitest","workspace":test_workspace},
                    {"type":"layer","name":"testview4unitest","workspace":test_workspace}
                ]
            }
            self.geoserver.update_layergroup(test_workspace,test_groupname,parameters,create=True)
            self.assertTrue(self.geoserver.has_layergroup(test_workspace,test_groupname),"The layergroup({}:{}) should have been created".format(test_workspace,test_groupname))

            layergroups = self.geoserver.list_layergroups(test_workspace)
            self.assertEqual(len(layergroups),1,"Only one layergroup({}:{}) was created".format(test_workspace,test_groupname))
            print("layergroups = {}".format(layergroups))

            layergroupdata = self.geoserver.get_layergroup(test_workspace,test_groupname)
            self.assertTrue(layergroupdata,"The layergroup({}:{}) should have been created".format(test_workspace,test_groupname))
            print("{}\n{}".format(test_groupname,json.dumps(layergroupdata,indent=4)))

            for key in ("workspace","name","title","bounds","keywords","layers"):
                print("{} = {}".format(key,self.geoserver.get_layergroupfield(layergroupdata,key)))

            parameters = {
                "title": "test layergroup for unitest, changed",
                "abstract": "test layergroup for unitest, changed",
                "keywords": ["unitest1","layergroup1"],
                "layers":[
                    {"type":"layer","name":"testview4unitest","workspace":test_workspace},
                    {"type":"layer","name":"testft4unitest","workspace":test_workspace}
                ]
            }
            self.geoserver.update_layergroup(test_workspace,test_groupname,parameters,create=False)
 
            layergroupdata2 = self.geoserver.get_layergroup(test_workspace,test_groupname)
            self.assertTrue(layergroupdata2,"The layergroup({}:{}) should have been created".format(test_workspace,test_groupname))
            print("{}\n{}".format(test_groupname,json.dumps(layergroupdata2,indent=4)))

            self.assertEqual(self.geoserver.get_layergroupfield(layergroupdata2,"title"),parameters["title"],"The title({2}) of the layergroup({0}:{1}) should be '{3}'".format(test_workspace,test_groupname,self.geoserver.get_layergroupfield(layergroupdata2,"title"),parameters["title"]))

            self.assertEqual(self.geoserver.get_layergroupfield(layergroupdata2,"keywords"),parameters["keywords"],"The keywords({2}) of the layergroup({0}:{1}) should be '{3}'".format(test_workspace,test_groupname,self.geoserver.get_layergroupfield(layergroupdata2,"keywords"),parameters["keywords"]))

            self.assertEqual(self.geoserver.get_layergroupfield(layergroupdata2,"layers"),[("layer",test_workspace,layer["name"]) for layer in parameters["layers"]],"The keywords({2}) of the layergroup({0}:{1}) should be '{3}'".format(test_workspace,test_groupname,self.geoserver.get_layergroupfield(layergroupdata2,"keywords"),parameters["keywords"]))

            self.geoserver.delete_layergroup(test_workspace,test_groupname)
            self.assertTrue(not self.geoserver.has_layergroup(test_workspace,test_groupname),"The layergroup({}:{}) should have been deleted".format(test_workspace,test_groupname))

        finally:
            #delete the test workspace
            print("Delete the testing workspace({})".format(test_workspace))
            self.geoserver.delete_workspace(test_workspace,recurse=True)




if __name__ == "__main__":
    unittest.main()


import os
import filecmp
import time
from datetime import datetime
import json
import logging


from . import loggingconfig
from .geoservercompatibilitycheck import GeoserverCompatibilityCheck
from . import settings
from .geoserver import Geoserver
from .exceptions import *
from . import utils


logger = logging.getLogger("geoserver_rest.geoservercompatibilitycheck")
class GeoclusterCompatibilityCheck(GeoserverCompatibilityCheck):
    def __init__(self,geoserver_url,geoslave_url,geoserver_user,geoserver_password,requestheaders=None):
        super().__init__(geoserver_url,geoserver_user,geoserver_password,requestheaders=requestheaders)
        self.geoslave = Geoserver(geoslave_url,geoserver_user,geoserver_password,headers=requestheaders)
        self.sync_timeout = int(os.environ.get("GEOCLUSTER_SYNC_TIMEOUT",30))
        self.check_interval = 10

    def is_metadata_equal(self,masterdata,data):
        if masterdata and not data:
            return False
        if not masterdata and data:
            return False

        if isinstance(masterdata,dict):
            if not isinstance(data,dict):
                return False
            if len(masterdata) == 2 and masterdata.get("@key") == "passwd":
                return True

            for key,val in masterdata.items():
                if key.startswith("date"):
                    continue
                elif  key in ("advertised","password","_default"):
                    continue
                elif self.is_metadata_equal(val,data.get(key)):
                    continue
                else:
                    return False

            return True
                    
        elif isinstance(masterdata,(list,tuple)):
            if not isinstance(data,(list,tuple)):
                return False
            if len(masterdata) != len(data):
                return False
            for index  in range(len(masterdata)):
                if self.is_metadata_equal(masterdata[index],data[index]):
                    continue
                else:
                    return False

            return True
        elif isinstance(masterdata,str):
            if not isinstance(data,str):
                return False
            data = data.replace(self.geoslave.geoserver_url,self.geoserver.geoserver_url)
            return masterdata == data
        else:
            return masterdata == data

    def reset_checking_env(self):
        super().reset_checking_env()
        starttime = datetime.now()
        while True:
            timeout = (datetime.now() - starttime).total_seconds() > self.sync_timeout
            try:
                for role in self.geoslave.list_roles():
                    if role.endswith(self.sufix):
                        raise Exception("Failed to delete the testing role '{}'".format(role))
        
                for user in self.geoslave.list_users():
                    if user[0].endswith(self.sufix):
                        raise Exception("Failed to delete the testing user '{}'".format(user[0]))
        
                for usergroup in self.geoslave.list_usergroups():
                    if usergroup.endswith(self.sufix):
                        raise Exception("Failed to delete the testing usergroup '{}'".format(usergroup))
        
                for wsname in self.geoslave.list_workspaces():
                    if wsname.endswith(self.sufix):
                        raise Exception("Failed to delete the testing workspace '{}'".format(wsname))
                break
            except:
                if timeout:
                    raise
                else:
                    time.sleep(self.check_interval)
                    continue

        logger.debug("Succeed to reset checking env of geocluster slave server({})".format(self.geoslave.geoserver_url))

    def post_create_workspace(self,wsname):
        starttime = datetime.now()
        while (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
            if self.geoslave.has_workspace(wsname):
                return  [True,"Succeed to synchronize the changes of the workspace({1}) to geocluster slave server({0})".format(self.geoslave.geoserver_url,wsname)]
            else:
                time.sleep(self.check_interval)

        self.geoslave.reload()

        if not self.geoslave.has_workspace(wsname):
            raise Exception("post_create_workspace: Failed to reload the data for geocluster slave server({})".format(self.geoslave.geoserver_url))

        return  [False,"Failed to synchronize the changes of the workspace({1}) to geocluster slave server({0})".format(self.geoslave.geoserver_url,wsname)]

    def post_delete_workspace(self,wsname):
        starttime = datetime.now()
        while (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
            if not self.geoslave.has_workspace(wsname):
                return  [True,"Succeed to synchronize the deletion of the workspace({1}) to geocluster slave server({0})".format(self.geoslave.geoserver_url,wsname)]
            else:
                time.sleep(self.check_interval)

        self.geoslave.reload()

        if self.geoslave.has_workspace(wsname):
            raise Exception("post_delete_workspace: Failed to reload the data for geocluster slave server({})".format(self.geoslave.geoserver_url))

        return  [False,"Failed to synchronize the deletion to geocluster slave server({0})".format(self.geoslave.geoserver_url,wsname)]

    def post_create_localdatastore(self,wsname,storename,parameters):
        starttime = datetime.now()
        masterdata = self.geoserver.get_datastore(wsname,storename)
        data = None
        while (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
            try:
                data = self.geoslave.get_datastore(wsname,storename)
            except ResourceNotFound as ex:
                pass
            if data:
                if self.is_metadata_equal(masterdata,data):
                    return  [True,"Succeed to synchronize the changes of the datastore({1}) to geocluster slave server({0})".format(self.geoslave.geoserver_url,storename)]
                else:
                    self.geoslave.reload()
                    return  [False,"Failed to synchronize the changes of the datastore({1}) to geocluster slave server({0}),\n    master data = {2}\n    slave data = {3}".format(self.geoslave.geoserver_url,storename,json.dumps(masterdata,indent=4),json.dumps(data,indent=4))]
            else:
                time.sleep(self.check_interval)

        self.geoslave.reload()

        try:
            data1 = self.geoslave.get_datastore(wsname,storename)
        except ResourceNotFound as ex:
            data1 = None
        if not data1 or not self.is_metadata_equal(masterdata,data1):
            raise Exception("post_create_localdatastore: Failed to reload the data for geocluster slave server({})".format(self.geoslave.geoserver_url))

        return  [False,"Failed to synchronize the changes of the datastore({1}) to geocluster slave server({0})".format(self.geoslave.geoserver_url,storename)]


    def post_create_postgisdatastore(self,wsname,storename,parameters):
        return self.post_create_localdatastore(wsname,storename,parameters)
    
    def post_update_postgisdatastore(self,wsname,storename,parameters):
        starttime = datetime.now()
        masterdata = None
        data = None
        masterdata = self.geoserver.get_datastore(wsname,storename)
        while (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
            try:
                data = self.geoslave.get_datastore(wsname,storename)
            except ResourceNotFound as ex:
                pass
            if data:
                if self.is_metadata_equal(masterdata,data):
                    return  [True,"Succeed to synchronize the changes of the datastore({1}) to geocluster slave server({0})".format(self.geoslave.geoserver_url,storename)]
            time.sleep(self.check_interval)

        self.geoslave.reload()

        try:
            data1 = self.geoslave.get_datastore(wsname,storename)
        except ResourceNotFound as ex:
            data1 = None
        if not data1 or not self.is_metadata_equal(masterdata,data1):
            raise Exception("post_update_postgisdatastore: Failed to reload the data for geocluster slave server({})".format(self.geoslave.geoserver_url))

        return  [False,"Failed to synchronize the changes of the datastore({1}) to geocluster slave server({0}),\n    master data = {2}\n    slave data = {3}".format(self.geoslave.geoserver_url,storename,json.dumps(masterdata,indent=4),json.dumps(data,indent=4))]
    
    def post_delete_datastore(self,wsname,storename):
        starttime = datetime.now()
        while (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
            if not self.geoslave.has_datastore(wsname,storename):
                return  [True,"Succeed to synchronize the deletion of the datastore({1}) to geocluster slave server({0})".format(self.geoslave.geoserver_url,storename)]
            else:
                time.sleep(self.check_interval)

        self.geoslave.reload()

        if self.geoslave.has_datastore(wsname,storename):
            raise Exception("post_delete_datastore: Failed to reload the data for geocluster slave server({})".format(self.geoslave.geoserver_url))

        return  [False,"Failed to synchronize the deletion of the datastore({1}) to geocluster slave server({0})".format(self.geoslave.geoserver_url,storename)]

    def post_create_style(self,wsname,stylename,styleversion,styledata):
        starttime = datetime.now()
        masterdata = self.geoserver.get_style(wsname,stylename)
        data = None
        while (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
            try:
                data = self.geoslave.get_style(wsname,stylename)
            except ResourceNotFound as ex:
                pass
            if data:
                if self.is_metadata_equal(masterdata,data):
                    return  [True,"Succeed to synchronize the changes of the style({1}) to geocluster slave server({0})".format(self.geoslave.geoserver_url,stylename)]
                else:
                    self.geoslave.reload()
                    return  [False,"Failed to synchronize the changes of the style({1}) to geocluster slave server({0}),\n    master styledata = {2}\n    slave styledata = {3}".format(self.geoslave.geoserver_url,stylename,json.dumps(masterdata,indent=4),json.dumps(data,indent=4))]
            else:
                time.sleep(self.check_interval)

        self.geoslave.reload()

        try:
            data1 = self.geoslave.get_style(wsname,stylename)
        except ResourceNotFound as ex:
            data1 = None
        if not data1 or not self.is_metadata_equal(masterdata,data1):
            raise Exception("post_create_style: Failed to reload the data for geocluster slave server({})".format(self.geoslave.geoserver_url))

        return  [False,"Failed to synchronize the changes of the style({1}) to geocluster slave server({0})".format(self.geoslave.geoserver_url,stylename)]

    def post_update_style(self,wsname,layername,stylename,bbox,srs,wmsimage):
        masterdata = None
        data = None
        starttime = datetime.now()
        slaveimage = None
        while (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
            try:
                slaveimage = self.geoslave.get_map(wsname,layername,bbox,srs=srs,width=1024,height=1024,format="image/jpeg")
                if filecmp.cmp(slaveimage,wmsimage):
                    return  [True,"Succeed to synchronize the changes of the style({1}) to geocluster slave server({0})".format(self.geoslave.geoserver_url,stylename)]
            except :
                pass
            finally:
                utils.remove_file(slaveimage)
                slaveimage = None
            time.sleep(self.check_interval)

        self.geoslave.reload()

        try:
            slaveimage = self.geoslave.get_map(wsname,layername,bbox,srs=srs,width=1024,height=1024,format="image/jpeg")
            if not filecmp.cmp(slaveimage,wmsimage):
                raise Exception("post_update_style: Failed to reload the data for geocluster slave server({})".format(self.geoslave.geoserver_url))
        except :
            raise Exception("post_update_style: Failed to reload the data for geocluster slave server({})".format(self.geoslave.geoserver_url))
        finally:
            utils.remove_file(slaveimage)

        return  [False,"Failed to synchronize the changes of the style({1}) to geocluster slave server({0}),\n    master styledata = {2}\n    slave styledata = {3}".format(self.geoslave.geoserver_url,stylename,json.dumps(masterdata,indent=4),json.dumps(data,indent=4))]

    
    def post_delete_style(self,wsname,layername,stylename):
        starttime = datetime.now()
        while (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
            if not self.geoslave.has_style(wsname,stylename):
                return  [True,"Succeed to synchronize the deletion of the style({1}) to geocluster slave server({0})".format(self.geoslave.geoserver_url,stylename)]
            else:
                time.sleep(self.check_interval)

        self.geoslave.reload()

        if self.geoslave.has_style(wsname,stylename):
            raise Exception("post_delete_style: Failed to reload the data for geocluster slave server({})".format(self.geoslave.geoserver_url))

        return  [False,"Failed to synchronize the deletion of the style({1}) to geocluster slave server({0})".format(self.geoslave.geoserver_url,stylename)]

    def post_publish_featuretype_from_localdatastore(self,wsname,storename,layername,parameters):
        starttime = datetime.now()
        masterdata = self.geoserver.get_featuretype(wsname,layername,storename=storename)
        data = None
        while (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
            try:
                data = self.geoslave.get_featuretype(wsname,layername,storename=storename)
            except ResourceNotFound as ex:
                pass
            if data:
                if self.is_metadata_equal(masterdata,data):
                    return  [True,"Succeed to synchronize the changes of the featuretype({1}.{2}.{3}) to geocluster slave server({0})".format(self.geoslave.geoserver_url,wsname,storename,layername)]
                else:
                    self.geoslave.reload()
                    return  [False,"Failed to synchronize the changes of the featuretype({1}.{2}.{3}) to geocluster slave server({0}),\n    master featuretype data = {4}\n    slave featuretype data = {5}".format(self.geoslave.geoserver_url,wsname,storename,layername,json.dumps(masterdata,indent=4),json.dumps(data,indent=4))]
            else:
                time.sleep(self.check_interval)

        self.geoslave.reload()

        try:
            data1 = self.geoslave.get_featuretype(wsname,layername,storename=storename)
        except ResourceNotFound as ex:
            data1 = None
        if not data1 or not self.is_metadata_equal(masterdata,data1):
            raise Exception("post_publish_featuretype_from_localdatastore: Failed to reload the data for geocluster slave server({})".format(self.geoslave.geoserver_url))

        return  [False,"Failed to synchronize the changes of the featuretype({1}.{2}.{3}) to geocluster slave server({0})".format(self.geoslave.geoserver_url,wsname,storename,layername)]

    def post_publish_featuretype_from_postgisdatastore(self,wsname,storename,layername,parameters):
        return self.post_publish_featuretype_from_localdatastore(wsname,storename,layername,parameters)

    def post_update_featuretype(self,wsname,storename,layername):
        masterdata = None
        data = None
        starttime = datetime.now()
        masterdata = self.geoserver.get_featuretype(wsname,layername,storename=storename)
        while (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
            try:
                data = self.geoslave.get_featuretype(wsname,layername,storename=storename)
            except ResourceNotFound as ex:
                pass
            if data:
                if self.is_metadata_equal(masterdata,data):
                    return  [True,"Succeed to synchronize the changes of the featuretype({1}.{2}.{3}) to geocluster slave server({0})".format(self.geoslave.geoserver_url,wsname,storename,layername)]
            time.sleep(self.check_interval)

        self.geoslave.reload()

        try:
            data1 = self.geoslave.get_featuretype(wsname,layername,storename=storename)
        except ResourceNotFound as ex:
            data1 = None
        if not data1 or not self.is_metadata_equal(masterdata,data1):
            raise Exception("post_update_featuretype: Failed to reload the data for geocluster slave server({})".format(self.geoslave.geoserver_url))

        return  [False,"Failed to synchronize the changes of the featuretype({1}.{2}.{3}) to geocluster slave server({0}),\n    master featuretype data = {4}\n    slave featuretype data = {5}".format(self.geoslave.geoserver_url,wsname,storename,layername,json.dumps(masterdata,indent=4),json.dumps(data,indent=4))]

    def post_update_featuretype_styles(self,wsname,layername,defaultstyle,styles):
        masterdata = None
        data = None
        starttime = datetime.now()
        masterdata = self.geoserver.get_featuretype_styles(wsname,layername)
        while (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
            try:
                data = self.geoslave.get_featuretype_styles(wsname,layername)
            except ResourceNotFound as ex:
                pass
            if data:
                if self.is_metadata_equal(masterdata,data):
                    return  [True,"Succeed to synchronize the style settings of the featuretype({1}.{2}) to geocluster slave server({0})".format(self.geoslave.geoserver_url,wsname,layername)]
            time.sleep(self.check_interval)

        self.geoslave.reload()

        try:
            data1 = self.geoslave.get_featuretype_styles(wsname,layername)
        except ResourceNotFound as ex:
            data1 = None
        if not data1 or not self.is_metadata_equal(masterdata,data1):
            raise Exception("post_update_featuretype_styles: Failed to reload the data for geocluster slave server({})".format(self.geoslave.geoserver_url))

        return  [False,"Failed to synchronize the style settings of the featuretype({1}.{2}) to geocluster slave server({0}),\n    master featuretype styles = {3}\n    slave featuretype styles = {4}".format(self.geoslave.geoserver_url,wsname,layername,json.dumps(masterdata,indent=4),json.dumps(data,indent=4))]

    def post_delete_featuretype(self,wsname,storename,layername):
        starttime = datetime.now()
        while (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
            if not self.geoslave.has_featuretype(wsname,layername,storename=storename):
                return  [True,"Succeed to synchronize the deletion of the featuretype({1}.{2}.{3}) to geocluster slave server({0})".format(self.geoslave.geoserver_url,wsname,storename,layername)]
            else:
                time.sleep(self.check_interval)

        self.geoslave.reload()

        if self.geoslave.has_featuretype(wsname,layername,storename=storename):
            raise Exception("post_delete_featuretype: Failed to reload the data for geocluster slave server({})".format(self.geoslave.geoserver_url))

        return  [False,"Failed to synchronize the deletion of the featuretype({1}.{2}.{3}) to geocluster slave server({0})".format(self.geoslave.geoserver_url,wsname,storename,layername)]

    def post_create_wmsstore(self,wsname,storename,parameters):
        starttime = datetime.now()
        masterdata = self.geoserver.get_wmsstore(wsname,storename)
        data = None
        while (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
            try:
                data = self.geoslave.get_wmsstore(wsname,storename)
            except ResourceNotFound as ex:
                pass
            if data:
                if self.is_metadata_equal(masterdata,data):
                    return  [True,"Succeed to synchronize the changes of the wmsstore({1}.{2}) to geocluster slave server({0})".format(self.geoslave.geoserver_url,wsname,storename)]
                else:
                    self.geoslave.reload()
                    return  [False,"Failed to synchronize the changes of the wmsstore({1}.{2}) to geocluster slave server({0}),\n    master data = {3}\n    slave data = {4}".format(self.geoslave.geoserver_url,wsname,storename,json.dumps(masterdata,indent=4),json.dumps(data,indent=4))]
            else:
                time.sleep(self.check_interval)

        self.geoslave.reload()

        try:
            data1 = self.geoslave.get_wmsstore(wsname,storename)
        except ResourceNotFound as ex:
            data1 = None
        if not data1 or not self.is_metadata_equal(masterdata,data1):
            raise Exception("post_create_wmsstore: Failed to reload the data for geocluster slave server({})".format(self.geoslave.geoserver_url))

        return  [False,"Failed to synchronize the changes of the wmsstore({1}.{2}) to geocluster slave server({0})".format(self.geoslave.geoserver_url,wsname,storename)]

    def post_update_wmsstore(self,wsname,storename,parameters):
        masterdata = None
        data = None
        starttime = datetime.now()
        masterdata = self.geoserver.get_wmsstore(wsname,storename)
        while (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
            try:
                data = self.geoslave.get_wmsstore(wsname,storename)
            except ResourceNotFound as ex:
                pass
            if data:
                if self.is_metadata_equal(masterdata,data):
                    return  [True,"Succeed to synchronize the changes of the wmsstore({1}.{2}) to geocluster slave server({0})".format(self.geoslave.geoserver_url,wsname,storename)]
            time.sleep(self.check_interval)

        self.geoslave.reload()

        try:
            data1 = self.geoslave.get_wmsstore(wsname,storename)
        except ResourceNotFound as ex:
            data1 = None
        if not data1 or not self.is_metadata_equal(masterdata,data1):
            raise Exception("post_update_wmsstore: Failed to reload the data for geocluster slave server({})".format(self.geoslave.geoserver_url))

        return  [False,"Failed to synchronize the changes of the wmsstore({1}.{2}) to geocluster slave server({0}),\n    master data = {3}\n    slave data = {4}".format(self.geoslave.geoserver_url,wsname,storename,json.dumps(masterdata,indent=4),json.dumps(data,indent=4))]

    def post_delete_wmsstore(self,wsname,storename):
        starttime = datetime.now()
        while (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
            if not self.geoslave.has_wmsstore(wsname,storename):
                return  [True,"Succeed to synchronize the deletion of the wmsstore({1}.{2}) to geocluster slave server({0})".format(self.geoslave.geoserver_url,wsname,storename)]
            else:
                time.sleep(self.check_interval)

        self.geoslave.reload()

        if self.geoslave.has_wmsstore(wsname,storename):
            raise Exception("post_delete_wmsstore: Failed to reload the data for geocluster slave server({})".format(self.geoslave.geoserver_url))

        return  [False,"Failed to synchronize the deletion of the wmsstore({1}.{2}) to geocluster slave server({0})".format(self.geoslave.geoserver_url,wsname,storename)]

    def post_publish_wmslayer(self,wsname,storename,layername,parameters):
        starttime = datetime.now()
        masterdata = self.geoserver.get_wmslayer(wsname,layername,storename=storename)
        data = None
        while (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
            try:
                data = self.geoslave.get_wmslayer(wsname,layername,storename=storename)
            except ResourceNotFound as ex:
                pass
            if data:
                if self.is_metadata_equal(masterdata,data):
                    return  [True,"Succeed to synchronize the changes of the wmslayer({1}.{2}.{3}) to geocluster slave server({0})".format(self.geoslave.geoserver_url,wsname,storename,layername)]
                else:
                    self.geoslave.reload()
                    return  [False,"Failed to synchronize the changes of the wmsstore({1}.{2}.{3}) to geocluster slave server({0}),\n    master data = {4}\n    slave data = {5}".format(self.geoslave.geoserver_url,wsname,storename,layername,json.dumps(masterdata,indent=4),json.dumps(data,indent=4))]
            else:
                time.sleep(self.check_interval)

        self.geoslave.reload()

        try:
            data1 = self.geoslave.get_wmslayer(wsname,layername,storename=storename)
        except ResourceNotFound as ex:
            data1 = None
        if not data1 or not self.is_metadata_equal(masterdata,data1):
            raise Exception("post_publish_wmslayer: Failed to reload the data for geocluster slave server({})".format(self.geoslave.geoserver_url))

        return  [False,"Failed to synchronize the changes of the wmsstore({1}.{2}.{3}) to geocluster slave server({0})".format(self.geoslave.geoserver_url,wsname,storename,layername)]

    def post_update_wmslayer(self,wsname,storename,layername,parameters):
        masterdata = None
        data =  None
        starttime = datetime.now()
        masterdata = self.geoserver.get_wmslayer(wsname,layername,storename=storename)
        while (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
            try:
                data = self.geoslave.get_wmslayer(wsname,layername,storename=storename)
            except ResourceNotFound as ex:
                pass
            if data:
                if self.is_metadata_equal(masterdata,data):
                    return  [True,"Succeed to synchronize the changes of the wmslayer({1}.{2}.{3}) to geocluster slave server({0})".format(self.geoslave.geoserver_url,wsname,storename,layername)]
            time.sleep(self.check_interval)

        self.geoslave.reload()

        try:
            data1 = self.geoslave.get_wmslayer(wsname,layername,storename=storename)
        except ResourceNotFound as ex:
            data1 = None
        if not data1 or not self.is_metadata_equal(masterdata,data1):
            raise Exception("post_update_wmslayer: Failed to reload the data for geocluster slave server({})".format(self.geoslave.geoserver_url))

        return  [False,"Failed to synchronize the changes of the wmsstore({1}.{2}.{3}) to geocluster slave server({0}),\n    master data = {4}\n    slave data = {5}".format(self.geoslave.geoserver_url,wsname,storename,layername,json.dumps(masterdata,indent=4),json.dumps(data,indent=4))]

    
    def post_delete_wmslayer(self,wsname,storename,layername):
        starttime = datetime.now()
        while (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
            if not self.geoslave.has_wmslayer(wsname,layername,storename=storename):
                return  [True,"Succeed to synchronize the deletion of the wmslayer({1}.{2}.{3}) to geocluster slave server({0})".format(self.geoslave.geoserver_url,wsname,storename,layername)]
            else:
                time.sleep(self.check_interval)

        self.geoslave.reload()

        if self.geoslave.has_wmslayer(wsname,layername,storename=storename):
            raise Exception("post_delete_wmslayer: Failed to reload the data for geocluster slave server({})".format(self.geoslave.geoserver_url))

        return  [False,"Failed to synchronize the deletion of the wmsstore({1}.{2}.{3}) to geocluster slave server({0})".format(self.geoslave.geoserver_url,wsname,storename,layername)]

    def post_create_layergroup(self,wsname,groupname,parameters):
        starttime = datetime.now()
        masterdata = self.geoserver.get_layergroup(wsname,groupname)
        data = None
        while (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
            try:
                data = self.geoslave.get_layergroup(wsname,groupname)
            except ResourceNotFound as ex:
                pass
            if data:
                if self.is_metadata_equal(masterdata,data):
                    return  [True,"Succeed to synchronize the changes of the layergroup({1}.{2}) to geocluster slave server({0})".format(self.geoslave.geoserver_url,wsname,groupname)]
                else:
                    self.geoslave.reload()
                    return  [False,"Failed to synchronize the changes of the layergroup({1}.{2}) to geocluster slave server({0}),\n    master data = {3}\n    slave data = {4}".format(self.geoslave.geoserver_url,wsname,groupname,json.dumps(masterdata,indent=4),json.dumps(data,indent=4))]
            else:
                time.sleep(self.check_interval)

        self.geoslave.reload()

        try:
            data1 = self.geoslave.get_layergroup(wsname,groupname)
        except ResourceNotFound as ex:
            data1 = None
        if not data1 or not self.is_metadata_equal(masterdata,data1):
            raise Exception("post_create_layergroup: Failed to reload the data for geocluster slave server({})".format(self.geoslave.geoserver_url))

        return  [False,"Failed to synchronize the changes of the layergroup({1}.{2}) to geocluster slave server({0})".format(self.geoslave.geoserver_url,wsname,groupname)]

    def post_update_layergroup(self,wsname,groupname,parameters):
        masterdata = None
        data = None
        starttime = datetime.now()
        masterdata = self.geoserver.get_layergroup(wsname,groupname)
        while (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
            try:
                data = self.geoslave.get_layergroup(wsname,groupname)
            except ResourceNotFound as ex:
                pass
            if data:
                if self.is_metadata_equal(masterdata,data):
                    return  [True,"Succeed to synchronize the changes of the layergroup({1}.{2}) to geocluster slave server({0})".format(self.geoslave.geoserver_url,wsname,groupname)]
            time.sleep(self.check_interval)

        self.geoslave.reload()

        try:
            data1 = self.geoslave.get_layergroup(wsname,groupname)
        except ResourceNotFound as ex:
            data1 = None
        if not data1 or not self.is_metadata_equal(masterdata,data1):
            raise Exception("post_update_layergroup: Failed to reload the data for geocluster slave server({})".format(self.geoslave.geoserver_url))

        return  [False,"Failed to synchronize the changes of the layergroup({1}.{2}) to geocluster slave server({0}),\n    master data = {3}\n    slave data = {4}".format(self.geoslave.geoserver_url,wsname,groupname,json.dumps(masterdata,indent=4),json.dumps(data,indent=4))]

    def post_delete_layergroup(self,wsname,groupname):
        starttime = datetime.now()
        while (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
            if not self.geoslave.has_layergroup(wsname,groupname):
                return  [True,"Succeed to synchronize the deletion of the layergroup({1}.{2}) to geocluster slave server({0})".format(self.geoslave.geoserver_url,wsname,groupname)]
            else:
                time.sleep(self.check_interval)

        self.geoslave.reload()

        if self.geoslave.has_layergroup(wsname,groupname):
            raise Exception("post_delete_layergroup: Failed to reload the data for geocluster slave server({})".format(self.geoslave.geoserver_url))

        return  [False,"Failed to synchronize the deletion of the layergroup({1}.{2}) to geocluster slave server({0})".format(self.geoslave.geoserver_url,wsname,groupname)]

    def post_create_wmtslayer(self,wsname,layername,parameters):
        starttime = datetime.now()
        masterdata = self.geoserver.get_gwclayer(wsname,layername)
        data = None
        while (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
            try:
                data = self.geoslave.get_gwclayer(wsname,layername)
            except ResourceNotFound as ex:
                pass
            if data:
                if self.is_metadata_equal(masterdata,data):
                    return  [True,"Succeed to synchronize the changes of the gwclayer({1}.{2}) to geocluster slave server({0})".format(self.geoslave.geoserver_url,wsname,layername)]
                else:
                    self.geoslave.reload()
                    return  [False,"Failed to synchronize the changes of the gwclayer({1}.{2}) to geocluster slave server({0}),\n    master data = {3}\n    slave data = {4}".format(self.geoslave.geoserver_url,wsname,layername,json.dumps(masterdata,indent=4),json.dumps(data,indent=4))]
            else:
                time.sleep(self.check_interval)

        self.geoslave.reload()

        try:
            data1 = self.geoslave.get_gwclayer(wsname,layername)
        except ResourceNotFound as ex:
            data1 = None
        if not data1 or not self.is_metadata_equal(masterdata,data1):
            raise Exception("post_create_wmtslayer: Failed to reload the data for geocluster slave server({})".format(self.geoslave.geoserver_url))

        return  [False,"Failed to synchronize the changes of the gwclayer({1}.{2}) to geocluster slave server({0})".format(self.geoslave.geoserver_url,wsname,layername)]

    def post_update_wmtslayer(self,wsname,layername,parameters):
        masterdata = None
        data = None
        starttime = datetime.now()
        masterdata = self.geoserver.get_gwclayer(wsname,layername)
        while (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
            try:
                data = self.geoslave.get_gwclayer(wsname,layername)
            except ResourceNotFound as ex:
                pass
            if data:
                if self.is_metadata_equal(masterdata,data):
                    return  [True,"Succeed to synchronize the changes of the gwclayer({1}.{2}) to geocluster slave server({0})".format(self.geoslave.geoserver_url,wsname,layername)]
            time.sleep(self.check_interval)

        self.geoslave.reload()

        try:
            data1 = self.geoslave.get_gwclayer(wsname,layername)
        except ResourceNotFound as ex:
            data1 = None
        if not data1 or not self.is_metadata_equal(masterdata,data1):
            raise Exception("post_update_wmtslayer: Failed to reload the data for geocluster slave server({})".format(self.geoslave.geoserver_url))

        return  [False,"Failed to synchronize the changes of the gwclayer({1}.{2}) to geocluster slave server({0}),\n    master data = {3}\n    slave data = {4}".format(self.geoslave.geoserver_url,wsname,layername,json.dumps(masterdata,indent=4),json.dumps(data,indent=4))]
        
    def post_delete_wmtslayer(self,wsname,layername):
        starttime = datetime.now()
        while (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
            if not self.geoslave.has_gwclayer(wsname,layername):
                return  [True,"Succeed to synchronize the deletion of the gwclayer({1}.{2}) to geocluster slave server({0})".format(self.geoslave.geoserver_url,wsname,layername)]
            else:
                time.sleep(self.check_interval)

        self.geoslave.reload()

        if self.geoslave.has_gwclayer(wsname,layername):
            raise Exception("post_delete_wmtslayer: Failed to reload the data for geocluster slave server({})".format(self.geoslave.geoserver_url))

        return  [False,"Failed to synchronize the deletion of the gwclayer({1}.{2}) to geocluster slave server({0})".format(self.geoslave.geoserver_url,wsname,layername)]

    def post_get_original_tile(self,wsname,layername,tile):
        slavetile = None
        try:
            slavetile = self.geoslave.get_tile(wsname,layername)
            if filecmp.cmp(slavetile,tile):
                return  [True,"The tile of the layer({1}.{2}) in geocluster slave server({0}) matches the tile in master server".format(self.geoslave.geoserver_url,wsname,layername)]
            else:
                return  [False,"The tile of the layer({1}.{2}) in geocluster slave server({0}) doesn't match the tile in master server".format(self.geoslave.geoserver_url,wsname,layername)]
        finally:
            utils.remove_file(slavetile)


    def post_get_tile_beforeexpire(self,wsname,layername,tile):
        slavetile = None
        try:
            slavetile = self.geoslave.get_tile(wsname,layername)
            if filecmp.cmp(slavetile,tile):
                return  [True,"The cached tile of the layer({1}.{2}) in geocluster slave server({0}) matches the tile in master server".format(self.geoslave.geoserver_url,wsname,layername)]
            else:
                return  [False,"The cached tile of the layer({1}.{2}) in geocluster slave server({0}) doesn't match the tile in master server".format(self.geoslave.geoserver_url,wsname,layername)]
        finally:
            utils.remove_file(slavetile)

    def post_gwccache_expire(self,wsname,layername,tile):
        slavetile = None
        try:
            slavetile = self.geoslave.get_tile(wsname,layername)
            if filecmp.cmp(slavetile,tile):
                return  [True,"The expired and regenerated tile of the layer({1}.{2}) in geocluster slave server({0}) matches the tile in master server".format(self.geoslave.geoserver_url,wsname,layername)]
            else:
                return  [False,"The expired and regenerated tile of the layer({1}.{2}) in geocluster slave server({0}) doesn't match the tile in master server".format(self.geoslave.geoserver_url,wsname,layername)]
        finally:
            utils.remove_file(slavetile)

    def post_get_tile_beforeclear(self,wsname,layername,tile):
        slavetile = None
        try:
            slavetile = self.geoslave.get_tile(wsname,layername)
            if filecmp.cmp(slavetile,tile):
                return  [True,"The cached tile of the layer({1}.{2}) in geocluster slave server({0}) matches the tile in master server".format(self.geoslave.geoserver_url,wsname,layername)]
            else:
                return  [False,"The cached tile of the layer({1}.{2}) in geocluster slave server({0}) doesn't match the tile in master server".format(self.geoslave.geoserver_url,wsname,layername)]
        finally:
            utils.remove_file(slavetile)

    def post_empty_gwccache(self,wsname,layername,tile):
        starttime = datetime.now()
        slavetile = None
        while (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
            try:
                slavetile = self.geoslave.get_tile(wsname,layername)
                if filecmp.cmp(slavetile,tile):
                    return  [True,"The cleared and regenerated tile of the layer({1}.{2}) in geocluster slave server({0}) matches the tile in master server".format(self.geoslave.geoserver_url,wsname,layername)]
                else:
                    time.sleep(30)
            finally:
                utils.remove_file(slavetile)
                slavetile = None


        self.geoslave.empty_gwclayer(wsname,layername)
        try:
            slavetile = self.geoslave.get_tile(wsname,layername)
            if not filecmp.cmp(slavetile,tile):
                raise Exception("post_empty_gwccache: Failed to empty gwc cache for geocluster slave server({})".format(self.geoslave.geoserver_url))
        finally:
            utils.remove_file(slavetile)

        return  [False,"The cleared and regenerated tile of the layer({1}.{2}) in geocluster slave server({0}) doesn't match the tile in master server".format(self.geoslave.geoserver_url,wsname,layername)]


    def post_create_usergroup(self,groupname):
        starttime = datetime.now()
        while (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
            if self.geoslave.has_usergroup(groupname):
                return  [True,"Succeed to synchronize the creation of the usergroup({1}) to geocluster slave server({0})".format(self.geoslave.geoserver_url,groupname)]
            else:
                time.sleep(self.check_interval)

        self.geoslave.reload()

        if not self.geoslave.has_usergroup(groupname):
            raise Exception("post_create_usergroup: Failed to reload the data for geocluster slave server({})".format(self.geoslave.geoserver_url))

        return  [False,"Failed to synchronize the creation of the usergroup({1}) to geocluster slave server({0})".format(self.geoslave.geoserver_url,groupname)]

    def post_delete_usergroup(self,groupname):
        starttime = datetime.now()
        while (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
            if not self.geoslave.has_usergroup(groupname):
                return  [True,"Succeed to synchronize the deletion of the usergroup({1}) to geocluster slave server({0})".format(self.geoslave.geoserver_url,groupname)]
            else:
                time.sleep(self.check_interval)

        self.geoslave.reload()

        if self.geoslave.has_usergroup(groupname):
            raise Exception("post_delete_usergroup: Failed to reload the data for geocluster slave server({})".format(self.geoslave.geoserver_url))

        return  [False,"Failed to synchronize the deletion of the usergroup({1}) to geocluster slave server({0})".format(self.geoslave.geoserver_url,groupname)]

    def post_create_user(self,user,password,enable):
        starttime = datetime.now()
        result = None
        while (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
            data = None
            try:
                data = self.geoslave.get_user(user)
            except ObjectNotFound as ex:
                pass
            if data:
                if data[1] != enable:
                    result =  [False,"Failed to synchronize the enable status({2}) of the user({1}) to geocluster slave server({0})".format(self.geoslave.geoserver_url,user,enable)]
                    break
                if not self.geoslave.login(user,password):
                    result = [False,"Failed to synchronize the password of the user({1}) to geocluster slave server({0})".format(self.geoslave.geoserver_url,user)]
                    break
                return  [True,"Succeed to synchronize the changes of the user({1}) to geocluster slave server({0})".format(self.geoslave.geoserver_url,user)]
            else:
                time.sleep(self.check_interval)


        self.geoslave.reload()

        try:
            data1 = self.geoslave.get_user(user)
        except ResourceNotFound as ex:
            data1 = None
        if not data1 or data1[1] != enable or not self.geoslave.login(user,password):
            raise Exception("post_create_user: Failed to reload the data for geocluster slave server({})".format(self.geoslave.geoserver_url))

        return  result or [False,"Failed to synchronize the changes of the user({1}) to geocluster slave server({0})".format(self.geoslave.geoserver_url,user)]

    def post_enable_user(self,user,enable):
        starttime = datetime.now()
        while (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
            data = None
            try:
                data = self.geoslave.get_user(user)
            except ObjectNotFound as ex:
                pass
            if data:
                if data[1] == enable:
                    return  [True,"Succeed to synchronize the enable status({2}) of the user({1}) to geocluster slave server({0})".format(self.geoslave.geoserver_url,user,enable)]
            time.sleep(self.check_interval)

        self.geoslave.reload()

        try:
            data1 = self.geoslave.get_user(user)
        except ResourceNotFound as ex:
            data1 = None
        if not data1 or data1[1] != enable :
            raise Exception("post_enable_user: Failed to reload the data for geocluster slave server({})".format(self.geoslave.geoserver_url))

        return  [False,"Failed to synchronize the enable status({2}) of the user({1}) to geocluster slave server({0})".format(self.geoslave.geoserver_url,user,enable)]

    def post_change_userpassword(self,user,oldpassword,newpassword):
        starttime = datetime.now()
        while (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
            if self.geoslave.login(user,newpassword) :
                return  [True,"Succeed to synchronize the changes of the user({1}) to geocluster slave server({0})".format(self.geoslave.geoserver_url,user)]
            else:
                time.sleep(self.sync_timeout)
                
        self.geoslave.reload()

        try:
            data1 = self.geoslave.get_user(user)
        except ResourceNotFound as ex:
            data1 = None
        if not data1 or not self.geoslave.login(user,newpassword) :
            raise Exception("post_change_userpassword: Failed to reload the data for geocluster slave server({})".format(self.geoslave.geoserver_url))

        return  [False,"Failed to synchronize the password of the user({1}) to geocluster slave server({0})".format(self.geoslave.geoserver_url,user)]

    def post_delete_user(self,user):
        starttime = datetime.now()
        while (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
            if not self.geoslave.has_user(user):
                return  [True,"Succeed to synchronize the deletion of the user({1}) to geocluster slave server({0})".format(self.geoslave.geoserver_url,user)]
            else:
                time.sleep(self.check_interval)

        self.geoslave.reload()

        if self.geoslave.has_user(user):
            raise Exception("post_delete_user: Failed to reload the data for geocluster slave server({})".format(self.geoslave.geoserver_url))

        return  [False,"Failed to synchronize the deletion of the user({1}) to geocluster slave server({0})".format(self.geoslave.geoserver_url,user)]

    def post_add_user_to_group(self,user,group):
        starttime = datetime.now()
        while (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
            if self.geoslave.user_in_group(user,group):
                return  [True,"Succeed to synchronize the operation of adding the user({1}) to usergroup({2}) to geocluster slave server({0})".format(self.geoslave.geoserver_url,user,group)]
            else:
                time.sleep(self.check_interval)

        self.geoslave.reload()

        if not self.geoslave.user_in_group(user,group):
            raise Exception("post_add_user_to_group: Failed to reload the data for geocluster slave server({})".format(self.geoslave.geoserver_url))

        return  [False,"Failed to synchronize the operation of adding the user({1}) to usergroup({2}) to geocluster slave server({0})".format(self.geoslave.geoserver_url,user,group)]

    def post_delete_user_from_group(self,user,group):
        starttime = datetime.now()
        while (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
            if not self.geoslave.user_in_group(user,group):
                return  [True,"Succeed to synchronize the operation of deleting the user({1}) from usergroup({2}) to geocluster slave server({0})".format(self.geoslave.geoserver_url,user,group)]
            else:
                time.sleep(self.check_interval)

        self.geoslave.reload()

        if self.geoslave.user_in_group(user,group):
            raise Exception("post_delete_user_from_group: Failed to reload the data for geocluster slave server({})".format(self.geoslave.geoserver_url))

        return  [False,"Failed to synchronize the operation of deleting the user({1}) from usergroup({2}) to geocluster slave server({0})".format(self.geoslave.geoserver_url,user,group)]


    def post_create_role(self,role):
        starttime = datetime.now()
        while (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
            if self.geoslave.has_role(role):
                return  [True,"Succeed to synchronize the creation of the role({1}) to geocluster slave server({0})".format(self.geoslave.geoserver_url,role)]
            else:
                time.sleep(self.check_interval)

        self.geoslave.reload()

        if not self.geoslave.has_role(role):
            raise Exception("post_create_role: Failed to reload the data for geocluster slave server({})".format(self.geoslave.geoserver_url))

        return  [False,"Failed to synchronize the creation of the role({1}) to geocluster slave server({0})".format(self.geoslave.geoserver_url,role)]

    def post_delete_role(self,role):
        starttime = datetime.now()
        while (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
            if not self.geoslave.has_role(role):
                return  [True,"Succeed to synchronize the deletion of the role({1}) to geocluster slave server({0})".format(self.geoslave.geoserver_url,role)]
            else:
                time.sleep(self.check_interval)

        self.geoslave.reload()

        if self.geoslave.has_role(role):
            raise Exception("post_delete_role: Failed to reload the data for geocluster slave server({})".format(self.geoslave.geoserver_url))

        return  [False,"Failed to synchronize the deletion of the role({1}) to geocluster slave server({0})".format(self.geoslave.geoserver_url,role)]

    def post_associate_role_with_user(self,role,user):
        starttime = datetime.now()
        while (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
            if self.geoslave.user_has_role(user,role):
                return  [True,"Succeed to synchronize the operation of associating the user({2}) with the role({1}) to geocluster slave server({0})".format(self.geoslave.geoserver_url,role,user)]
            else:
                time.sleep(self.check_interval)

        self.geoslave.reload()

        if not self.geoslave.user_has_role(user,role):
            raise Exception("post_associate_role_with_user: Failed to reload the data for geocluster slave server({})".format(self.geoslave.geoserver_url))

        return  [False,"Failed to synchronize the operation of associating the user({2}) with the role({1}) to geocluster slave server({0})".format(self.geoslave.geoserver_url,role,user)]

    def post_unassociate_role_with_user(self,role,user):
        starttime = datetime.now()
        while (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
            if not self.geoslave.user_has_role(user,role):
                return  [True,"Succeed to synchronize the operation of unassociating the user({2}) with the role({1}) to geocluster slave server({0})".format(self.geoslave.geoserver_url,role,user)]
            else:
                time.sleep(self.check_interval)

        self.geoslave.reload()

        if self.geoslave.user_has_role(user,role):
            raise Exception("post_unassociate_role_with_user: Failed to reload the data for geocluster slave server({})".format(self.geoslave.geoserver_url))

        return  [False,"Failed to synchronize the operation of unassociating the user({2}) with the role({1}) to geocluster slave server({0})".format(self.geoslave.geoserver_url,role,user)]

    def post_associate_role_with_usergroup(self,role,group):
        starttime = datetime.now()
        while (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
            if self.geoslave.usergroup_has_role(group,role):
                return  [True,"Succeed to synchronize the operation of associating the usergroup({2}) with the role({1}) to geocluster slave server({0})".format(self.geoslave.geoserver_url,role,group)]
            else:
                time.sleep(self.check_interval)

        self.geoslave.reload()

        if not self.geoslave.usergroup_has_role(group,role):
            raise Exception("post_associate_role_with_usergroup: Failed to reload the data for geocluster slave server({})".format(self.geoslave.geoserver_url))

        return  [False,"Failed to synchronize the operation of associating the usergroup({2}) with the role({1}) to geocluster slave server({0})".format(self.geoslave.geoserver_url,role,group)]

    def post_unassociate_role_with_usergroup(self,role,group):
        starttime = datetime.now()
        while (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
            if not self.geoslave.usergroup_has_role(group,role):
                return  [True,"Succeed to synchronize the operation of unassociating the usergroup({2}) with the role({1}) to geocluster slave server({0})".format(self.geoslave.geoserver_url,role,group)]
            else:
                time.sleep(self.check_interval)

        self.geoslave.reload()

        if self.geoslave.usergroup_has_role(group,role):
            raise Exception("post_unassociate_role_with_usergroup: Failed to reload the data for geocluster slave server({})".format(self.geoslave.geoserver_url))

        return  [False,"Failed to synchronize the operation of unassociating the usergroup({2}) with the role({1}) to geocluster slave server({0})".format(self.geoslave.geoserver_url,role,group)]

    def post_grant_layer_access_permission(self,access_rules):
        starttime = datetime.now()
        data = None
        masterdata = None
        masterdata = self.geoserver.get_layer_access_rules()
        while (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
            data = self.geoslave.get_layer_access_rules()
            if data:
                if self.is_metadata_equal(masterdata,data):
                    return  [True,"Succeed to synchronize the changes of the layer access permissions to geocluster slave server({0})".format(self.geoslave.geoserver_url)]
            time.sleep(self.check_interval)

        self.geoslave.reload()

        try:
            data1 = self.geoslave.get_layer_access_rules()
        except ResourceNotFound as ex:
            data1 = None
        if not data1 or not self.is_metadata_equal(masterdata,data1):
            raise Exception("post_grant_layer_access_permission: Failed to reload the data for geocluster slave server({})".format(self.geoslave.geoserver_url))

        return  [False,"Failed to synchronize the changes of the layer access permissions to geocluster slave server({0}),\n    master data = {1}\n    slave data = {2}".format(self.geoslave.geoserver_url,json.dumps(masterdata,indent=4),json.dumps(data,indent=4))]
        
    def post_revoke_layer_access_permission(self,delete_permissions):
        return self.post_grant_layer_access_permission(delete_permissions)

if __name__ == '__main__':
    geoserver_url = os.environ["GEOSERVER_URL"]
    geoslave_url = os.environ["GEOSLAVE_URL"]
    geoserver_user = os.environ.get("GEOSERVER_USER")
    geoserver_password = os.environ.get("GEOSERVER_PASSWORD")

    compatibilitycheck = GeoclusterCompatibilityCheck(geoserver_url,geoslave_url,geoserver_user,geoserver_password,settings.GET_REQUEST_HEADERS("GEOSERVER_REQUEST_HEADERS"))
    compatibilitycheck.run()


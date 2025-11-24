import os
import traceback
import filecmp
import time
from datetime import datetime
import json
import logging
import shutil


from . import loggingconfig
from .geoservercompatibilitycheck import GeoserverCompatibilityCheck
from . import settings
from .geoserver import Geoserver
from .exceptions import *
from . import utils


logger = logging.getLogger("geoserver_rest.geoservercompatibilitycheck")
class GeoclusterCompatibilityCheck(GeoserverCompatibilityCheck):
    def __init__(self,geoserver_url,geoslaves_url,geoserver_user,geoserver_password,requestheaders=None,ssl_verify=True):
        super().__init__(geoserver_url,geoserver_user,geoserver_password,requestheaders=requestheaders,ssl_verify=ssl_verify)
        self.geoslaves = [Geoserver(geoslave_url.strip(),geoserver_user,geoserver_password,headers=requestheaders) for geoslave_url in geoslaves_url.split(",") if geoslave_url.strip()]
        self.sync_timeout = int(os.environ.get("GEOCLUSTER_SYNC_TIMEOUT",30))
        self.check_interval = 10

    def is_metadata_equal(self,geoslave,masterdata,data):
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
                elif self.is_metadata_equal(geoslave,val,data.get(key)):
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
                if self.is_metadata_equal(geoslave,masterdata[index],data[index]):
                    continue
                else:
                    return False

            return True
        elif isinstance(masterdata,str):
            if not isinstance(data,str):
                return False
            data = data.replace(geoslave.geoserver_url,self.geoserver.geoserver_url)
            return masterdata == data
        else:
            return masterdata == data

    def _check_geoslave(self,func,*args):
        starttime = datetime.now()
        results = []
        for geoslave in self.geoslaves:
            result = func(geoslave,starttime,*args)
            if result:
                results.append(result)

        return results if results else None

    def post_reset_checking_env(self):
        return self._check_geoslave(self._post_reset_checking_env)

    def _post_reset_checking_env(self,geoslave,starttime):
        while True:
            timeout = (datetime.now() - starttime).total_seconds() > self.sync_timeout
            try:
                for role in geoslave.list_roles():
                    if role.endswith(self.sufix):
                        raise Exception("Failed to delete the testing role '{}'".format(role))
        
                for user in geoslave.list_users():
                    if user[0].endswith(self.sufix):
                        raise Exception("Failed to delete the testing user '{}'".format(user[0]))
        
                for usergroup in geoslave.list_usergroups():
                    if usergroup.endswith(self.sufix):
                        raise Exception("Failed to delete the testing usergroup '{}'".format(usergroup))
        
                for wsname in geoslave.list_workspaces():
                    if wsname.endswith(self.sufix):
                        raise Exception("Failed to delete the testing workspace '{}'".format(wsname))
                break
            except:
                if timeout:
                    raise
                else:
                    time.sleep(self.check_interval)
                    continue

        logger.debug("Succeed to reset checking env of geocluster slave server({})".format(geoslave.geoserver_url))


    def post_create_workspace(self,wsname):
        return self._check_geoslave(self._post_create_workspace,wsname)

    def _post_create_workspace(self,geoslave,starttime,wsname):
        while True:
            if geoslave.has_workspace(wsname):
                return  [True,"Succeed to synchronize the changes of the workspace({1}) to geocluster slave server({0})".format(geoslave.geoserver_url,wsname)]
            elif (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
                time.sleep(self.check_interval)
            else:
                break

        geoslave.reload()

        if not geoslave.has_workspace(wsname):
            raise Exception("post_create_workspace: Failed to reload the data for geocluster slave server({})".format(geoslave.geoserver_url))

        return  [False,"Failed to synchronize the changes of the workspace({1}) to geocluster slave server({0})".format(geoslave.geoserver_url,wsname)]

    def post_delete_workspace(self,wsname):
        return self._check_geoslave(self._post_delete_workspace,wsname)

    def _post_delete_workspace(self,geoslave,starttime,wsname):
        while True:
            if not geoslave.has_workspace(wsname):
                return  [True,"Succeed to synchronize the deletion of the workspace({1}) to geocluster slave server({0})".format(geoslave.geoserver_url,wsname)]
            elif (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
                time.sleep(self.check_interval)
            else:
                break

        geoslave.reload()

        if geoslave.has_workspace(wsname):
            raise Exception("post_delete_workspace: Failed to reload the data for geocluster slave server({})".format(geoslave.geoserver_url))

        return  [False,"Failed to synchronize the deletion to geocluster slave server({0})".format(geoslave.geoserver_url,wsname)]

    def post_create_localdatastore(self,wsname,storename,parameters):
        return self._check_geoslave(self._post_create_localdatastore,wsname,storename,parameters)

    def _post_create_localdatastore(self,geoslave,starttime,wsname,storename,parameters):
        masterdata = self.geoserver.get_datastore(wsname,storename)
        data = None
        while True:
            try:
                data = geoslave.get_datastore(wsname,storename)
            except ResourceNotFound as ex:
                pass
            if data:
                if self.is_metadata_equal(geoslave,masterdata,data):
                    return  [True,"Succeed to synchronize the changes of the datastore({1}) to geocluster slave server({0})".format(geoslave.geoserver_url,storename)]
                else:
                    geoslave.reload()
                    return  [False,"Failed to synchronize the changes of the datastore({1}) to geocluster slave server({0}),\n    master data = {2}\n    slave data = {3}".format(geoslave.geoserver_url,storename,json.dumps(masterdata,indent=4),json.dumps(data,indent=4))]
            if (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
                time.sleep(self.check_interval)
            else:
                break

        geoslave.reload()

        try:
            data1 = geoslave.get_datastore(wsname,storename)
        except ResourceNotFound as ex:
            data1 = None
        if not data1 or not self.is_metadata_equal(geoslave,masterdata,data1):
            raise Exception("post_create_localdatastore: Failed to reload the data for geocluster slave server({})".format(geoslave.geoserver_url))

        return  [False,"Failed to synchronize the changes of the datastore({1}) to geocluster slave server({0})".format(geoslave.geoserver_url,storename)]


    def post_create_postgisdatastore(self,wsname,storename,parameters):
        return self.post_create_localdatastore(wsname,storename,parameters)
    
    def post_update_postgisdatastore(self,wsname,storename,parameters):
        return self._check_geoslave(self._post_update_postgisdatastore,wsname,storename,parameters)

    def _post_update_postgisdatastore(self,geoslave,starttime,wsname,storename,parameters):
        masterdata = None
        data = None
        masterdata = self.geoserver.get_datastore(wsname,storename)
        while True:
            try:
                data = geoslave.get_datastore(wsname,storename)
            except ResourceNotFound as ex:
                pass
            if data:
                if self.is_metadata_equal(geoslave,masterdata,data):
                    return  [True,"Succeed to synchronize the changes of the datastore({1}) to geocluster slave server({0})".format(geoslave.geoserver_url,storename)]

            if (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
                time.sleep(self.check_interval)
            else:
                break

        geoslave.reload()

        try:
            data1 = geoslave.get_datastore(wsname,storename)
        except ResourceNotFound as ex:
            data1 = None
        if not data1 or not self.is_metadata_equal(geoslave,masterdata,data1):
            raise Exception("post_update_postgisdatastore: Failed to reload the data for geocluster slave server({})".format(geoslave.geoserver_url))

        return  [False,"Failed to synchronize the changes of the datastore({1}) to geocluster slave server({0}),\n    master data = {2}\n    slave data = {3}".format(geoslave.geoserver_url,storename,json.dumps(masterdata,indent=4),json.dumps(data,indent=4))]
    
    def post_delete_datastore(self,wsname,storename):
        return self._check_geoslave(self._post_delete_datastore,wsname,storename)

    def _post_delete_datastore(self,geoslave,starttime,wsname,storename):
        while True:
            if not geoslave.has_datastore(wsname,storename):
                return  [True,"Succeed to synchronize the deletion of the datastore({1}) to geocluster slave server({0})".format(geoslave.geoserver_url,storename)]
            elif (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
                time.sleep(self.check_interval)
            else:
                break

        geoslave.reload()

        if geoslave.has_datastore(wsname,storename):
            raise Exception("post_delete_datastore: Failed to reload the data for geocluster slave server({})".format(geoslave.geoserver_url))

        return  [False,"Failed to synchronize the deletion of the datastore({1}) to geocluster slave server({0})".format(geoslave.geoserver_url,storename)]

    def post_create_style(self,wsname,stylename,styleversion,styledata):
        return self._check_geoslave(self._post_create_style,wsname,stylename,styleversion,styledata)

    def _post_create_style(self,geoslave,starttime,wsname,stylename,styleversion,styledata):
        masterdata = self.geoserver.get_style(wsname,stylename)
        mastersld = self.geoserver.get_sld(wsname,stylename)
        data = None
        sld = None
        while True:
            try:
                data = geoslave.get_style(wsname,stylename)
                sld = geoslave.get_sld(wsname,stylename)
            except ResourceNotFound as ex:
                pass
            if data:
                if self.is_metadata_equal(geoslave,masterdata,data) and sld == mastersld:
                    return  [True,"Succeed to synchronize the changes of the style({1}) to geocluster slave server({0})".format(geoslave.geoserver_url,stylename)]
                else:
                    geoslave.reload()
                    return  [False,"Failed to synchronize the changes of the style({1}) to geocluster slave server({0}),\n    master metadata= {2}\n    slave metadata = {3}\n    master sld= {4}\n    slave sld={5}".format(geoslave.geoserver_url,stylename,json.dumps(masterdata,indent=4),json.dumps(data,indent=4),mastersld,sld)]

            if (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
                time.sleep(self.check_interval)
            else:
                break

        geoslave.reload()

        try:
            data1 = geoslave.get_style(wsname,stylename)
            sld1 = geoslave.get_sld(wsname,stylename)
        except ResourceNotFound as ex:
            data1 = None
            sld1 = None
        if not data1 or not self.is_metadata_equal(geosalve,masterdata,data1) or mastersld != sld1:
            raise Exception("post_create_style: Failed to reload the data for geocluster slave server({})".format(geoslave.geoserver_url))

        return  [False,"Failed to synchronize the changes of the style({1}) to geocluster slave server({0})".format(geoslave.geoserver_url,stylename)]

    def post_update_style(self,wsname,layername,stylename,bbox,srs,wmsimage):
        return self._check_geoslave(self._post_update_style,wsname,layername,stylename,bbox,srs,wmsimage)

    def _post_update_style(self,geoslave,starttime,wsname,layername,stylename,bbox,srs,wmsimage):
        slaveimage = None
        now = datetime.now().strftime("%Y%m%d%H%M%S")
        masterimg = None
        mastersld = self.geoserver.get_sld(wsname,stylename)
        slaveimg = None
        slavesld = None
        while True:
            try:
                slaveimage = geoslave.get_map(wsname,layername,bbox,srs=srs,width=1024,height=1024,format="image/jpeg")
                slavesld = geoslave.get_sld(wsname,stylename)
                if filecmp.cmp(slaveimage,wmsimage):
                    utils.remove_file(slaveimg) and mastersld == slavesld
                    slaveimg = None
                    return  [True,"Succeed to synchronize the changes of the style({1}) to geocluster slave server({0})".format(geoslave.geoserver_url,stylename)]
                else:
                    slaveimg = "./{}_{}_{}_slave.jpg".format(layername,stylename,now)
                    shutil.copy(slaveimage,slaveimg)
            except :
                traceback.print_exc()
                pass
            finally:
                utils.remove_file(slaveimage)
                slaveimage = None
            if (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
                time.sleep(self.check_interval)
            else:
                break

        if slaveimg:
            masterimg = "./{}_{}_{}_master.jpg".format(layername,stylename,now)
            shutil.copy(wmsimage,masterimg)

        geoslave.reload()

        try:
            slaveimage = geoslave.get_map(wsname,layername,bbox,srs=srs,width=1024,height=1024,format="image/jpeg")
            slavesld1 = geoslave.get_sld(wsname,stylename)
            if not filecmp.cmp(slaveimage,wmsimage) or mastersld != slavesld1:
                raise Exception("post_update_style: Failed to reload the data for geocluster slave server({})".format(geoslave.geoserver_url))
        except :
            raise Exception("post_update_style: Failed to reload the data for geocluster slave server({})".format(geoslave.geoserver_url))
        finally:
            utils.remove_file(slaveimage)
        
        if slaveimg:
            return  [False,"Failed to synchronize the changes of the style({1}) to geocluster slave server({0}),\n    master image = {2}\n    slave image = {3}\n    master sld= {4}\n    slave sld={5}".format(geoslave.geoserver_url,stylename,masterimg,slaveimg,mastersld,slavesld)]
        else:
            return  [False,"Failed to synchronize the changes of the style({1}) to geocluster slave server({0})".format(geoslave.geoserver_url,stylename)]

    
    def post_delete_style(self,wsname,layername,stylename):
        return self._check_geoslave(self._post_delete_style,wsname,layername,stylename)

    def _post_delete_style(self,geoslave,starttime,wsname,layername,stylename):
        while True:
            if not geoslave.has_style(wsname,stylename):
                return  [True,"Succeed to synchronize the deletion of the style({1}) to geocluster slave server({0})".format(geoslave.geoserver_url,stylename)]
            elif (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
                time.sleep(self.check_interval)
            else:
                break

        geoslave.reload()

        if geoslave.has_style(wsname,stylename):
            raise Exception("post_delete_style: Failed to reload the data for geocluster slave server({})".format(geoslave.geoserver_url))

        return  [False,"Failed to synchronize the deletion of the style({1}) to geocluster slave server({0})".format(geoslave.geoserver_url,stylename)]

    def post_publish_featuretype_from_localdatastore(self,wsname,storename,layername,parameters):
        return self._check_geoslave(self._post_publish_featuretype_from_localdatastore,wsname,storename,layername,parameters)

    def _post_publish_featuretype_from_localdatastore(self,geoslave,starttime,wsname,storename,layername,parameters):
        masterdata = self.geoserver.get_featuretype(wsname,layername,storename=storename)
        data = None
        while True:
            try:
                data = geoslave.get_featuretype(wsname,layername,storename=storename)
            except ResourceNotFound as ex:
                pass
            if data:
                if self.is_metadata_equal(geoslave,masterdata,data):
                    return  [True,"Succeed to synchronize the changes of the featuretype({1}.{2}.{3}) to geocluster slave server({0})".format(geoslave.geoserver_url,wsname,storename,layername)]
                else:
                    geoslave.reload()
                    return  [False,"Failed to synchronize the changes of the featuretype({1}.{2}.{3}) to geocluster slave server({0}),\n    master featuretype data = {4}\n    slave featuretype data = {5}".format(geoslave.geoserver_url,wsname,storename,layername,json.dumps(masterdata,indent=4),json.dumps(data,indent=4))]

            if (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
                time.sleep(self.check_interval)
            else:
                break

        geoslave.reload()

        try:
            data1 = geoslave.get_featuretype(wsname,layername,storename=storename)
        except ResourceNotFound as ex:
            data1 = None
        if not data1 or not self.is_metadata_equal(geoslave,masterdata,data1):
            raise Exception("post_publish_featuretype_from_localdatastore: Failed to reload the data for geocluster slave server({})".format(geoslave.geoserver_url))

        return  [False,"Failed to synchronize the changes of the featuretype({1}.{2}.{3}) to geocluster slave server({0})".format(geoslave.geoserver_url,wsname,storename,layername)]

    def post_publish_featuretype_from_postgisdatastore(self,wsname,storename,layername,parameters):
        return self.post_publish_featuretype_from_localdatastore(wsname,storename,layername,parameters)

    def post_update_featuretype(self,wsname,storename,layername):
        return self._check_geoslave(self._post_update_featuretype,wsname,storename,layername)

    def _post_update_featuretype(self,geoslave,starttime,wsname,storename,layername):
        masterdata = None
        data = None
        masterdata = self.geoserver.get_featuretype(wsname,layername,storename=storename)
        while True:
            try:
                data = geoslave.get_featuretype(wsname,layername,storename=storename)
            except ResourceNotFound as ex:
                pass
            if data:
                if self.is_metadata_equal(geoslave,masterdata,data):
                    return  [True,"Succeed to synchronize the changes of the featuretype({1}.{2}.{3}) to geocluster slave server({0})".format(geoslave.geoserver_url,wsname,storename,layername)]
            if (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
                time.sleep(self.check_interval)
            else:
                break

        geoslave.reload()

        try:
            data1 = geoslave.get_featuretype(wsname,layername,storename=storename)
        except ResourceNotFound as ex:
            data1 = None
        if not data1 or not self.is_metadata_equal(geoslave,masterdata,data1):
            raise Exception("post_update_featuretype: Failed to reload the data for geocluster slave server({})".format(geoslave.geoserver_url))

        return  [False,"Failed to synchronize the changes of the featuretype({1}.{2}.{3}) to geocluster slave server({0}),\n    master featuretype data = {4}\n    slave featuretype data = {5}".format(geoslave.geoserver_url,wsname,storename,layername,json.dumps(masterdata,indent=4),json.dumps(data,indent=4))]

    def post_update_featuretype_styles(self,wsname,layername,defaultstyle,styles):
        return self._check_geoslave(self._post_update_featuretype_styles,wsname,layername,defaultstyle,styles)

    def _post_update_featuretype_styles(self,geoslave,starttime,wsname,layername,defaultstyle,styles):
        masterdata = None
        data = None
        masterdata = self.geoserver.get_featuretype_styles(wsname,layername)
        while True:
            try:
                data = geoslave.get_featuretype_styles(wsname,layername)
            except ResourceNotFound as ex:
                pass
            if data:
                if self.is_metadata_equal(geoslave,masterdata,data):
                    return  [True,"Succeed to synchronize the style settings of the featuretype({1}.{2}) to geocluster slave server({0})".format(geoslave.geoserver_url,wsname,layername)]
            if (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
                time.sleep(self.check_interval)
            else:
                break

        geoslave.reload()

        try:
            data1 = geoslave.get_featuretype_styles(wsname,layername)
        except ResourceNotFound as ex:
            data1 = None
        if not data1 or not self.is_metadata_equal(geoslave,masterdata,data1):
            raise Exception("post_update_featuretype_styles: Failed to reload the data for geocluster slave server({})".format(geoslave.geoserver_url))

        return  [False,"Failed to synchronize the style settings of the featuretype({1}.{2}) to geocluster slave server({0}),\n    master featuretype styles = {3}\n    slave featuretype styles = {4}".format(geoslave.geoserver_url,wsname,layername,json.dumps(masterdata,indent=4),json.dumps(data,indent=4))]

    def post_delete_featuretype(self,wsname,storename,layername):
        return self._check_geoslave(self._post_delete_featuretype,wsname,storename,layername)

    def _post_delete_featuretype(self,geoslave,starttime,wsname,storename,layername):
        while (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
            if not geoslave.has_featuretype(wsname,layername,storename=storename):
                return  [True,"Succeed to synchronize the deletion of the featuretype({1}.{2}.{3}) to geocluster slave server({0})".format(geoslave.geoserver_url,wsname,storename,layername)]
            elif (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
                time.sleep(self.check_interval)
            else:
                break

        geoslave.reload()

        if geoslave.has_featuretype(wsname,layername,storename=storename):
            raise Exception("post_delete_featuretype: Failed to reload the data for geocluster slave server({})".format(geoslave.geoserver_url))

        return  [False,"Failed to synchronize the deletion of the featuretype({1}.{2}.{3}) to geocluster slave server({0})".format(geoslave.geoserver_url,wsname,storename,layername)]

    def post_create_wmsstore(self,wsname,storename,parameters):
        return self._check_geoslave(self._post_create_wmsstore,wsname,storename,parameters)

    def _post_create_wmsstore(self,geoslave,starttime,wsname,storename,parameters):
        masterdata = self.geoserver.get_wmsstore(wsname,storename)
        data = None
        while True:
            try:
                data = geoslave.get_wmsstore(wsname,storename)
            except ResourceNotFound as ex:
                pass
            if data:
                if self.is_metadata_equal(geoslave,masterdata,data):
                    return  [True,"Succeed to synchronize the changes of the wmsstore({1}.{2}) to geocluster slave server({0})".format(geoslave.geoserver_url,wsname,storename)]
                else:
                    geoslave.reload()
                    return  [False,"Failed to synchronize the changes of the wmsstore({1}.{2}) to geocluster slave server({0}),\n    master data = {3}\n    slave data = {4}".format(geoslave.geoserver_url,wsname,storename,json.dumps(masterdata,indent=4),json.dumps(data,indent=4))]
            if (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
                time.sleep(self.check_interval)
            else:
                break


        geoslave.reload()

        try:
            data1 = geoslave.get_wmsstore(wsname,storename)
        except ResourceNotFound as ex:
            data1 = None
        if not data1 or not self.is_metadata_equal(geoslave,masterdata,data1):
            raise Exception("post_create_wmsstore: Failed to reload the data for geocluster slave server({})".format(geoslave.geoserver_url))

        return  [False,"Failed to synchronize the changes of the wmsstore({1}.{2}) to geocluster slave server({0})".format(geoslave.geoserver_url,wsname,storename)]

    def post_update_wmsstore(self,wsname,storename,parameters):
        return self._check_geoslave(self._post_update_wmsstore,wsname,storename,parameters)

    def _post_update_wmsstore(self,geoslave,starttime,wsname,storename,parameters):
        masterdata = None
        data = None
        masterdata = self.geoserver.get_wmsstore(wsname,storename)
        while True:
            try:
                data = geoslave.get_wmsstore(wsname,storename)
            except ResourceNotFound as ex:
                pass
            if data:
                if self.is_metadata_equal(geoslave,masterdata,data):
                    return  [True,"Succeed to synchronize the changes of the wmsstore({1}.{2}) to geocluster slave server({0})".format(geoslave.geoserver_url,wsname,storename)]
            if (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
                time.sleep(self.check_interval)
            else:
                break

        geoslave.reload()

        try:
            data1 = geoslave.get_wmsstore(wsname,storename)
        except ResourceNotFound as ex:
            data1 = None
        if not data1 or not self.is_metadata_equal(geoslave,masterdata,data1):
            raise Exception("post_update_wmsstore: Failed to reload the data for geocluster slave server({})".format(geoslave.geoserver_url))

        return  [False,"Failed to synchronize the changes of the wmsstore({1}.{2}) to geocluster slave server({0}),\n    master data = {3}\n    slave data = {4}".format(geoslave.geoserver_url,wsname,storename,json.dumps(masterdata,indent=4),json.dumps(data,indent=4))]

    def post_delete_wmsstore(self,wsname,storename):
        return self._check_geoslave(self._post_delete_wmsstore,wsname,storename)

    def _post_delete_wmsstore(self,geoslave,starttime,wsname,storename):
        while True:
            if not geoslave.has_wmsstore(wsname,storename):
                return  [True,"Succeed to synchronize the deletion of the wmsstore({1}.{2}) to geocluster slave server({0})".format(geoslave.geoserver_url,wsname,storename)]
            elif (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
                time.sleep(self.check_interval)
            else:
                break

        geoslave.reload()

        if geoslave.has_wmsstore(wsname,storename):
            raise Exception("post_delete_wmsstore: Failed to reload the data for geocluster slave server({})".format(geoslave.geoserver_url))

        return  [False,"Failed to synchronize the deletion of the wmsstore({1}.{2}) to geocluster slave server({0})".format(geoslave.geoserver_url,wsname,storename)]

    def post_publish_wmslayer(self,wsname,storename,layername,parameters):
        return self._check_geoslave(self._post_publish_wmslayer,wsname,storename,layername,parameters)

    def _post_publish_wmslayer(self,geoslave,starttime,wsname,storename,layername,parameters):
        masterdata = self.geoserver.get_wmslayer(wsname,layername,storename=storename)
        data = None
        while True:
            try:
                data = geoslave.get_wmslayer(wsname,layername,storename=storename)
            except ResourceNotFound as ex:
                pass
            if data:
                if self.is_metadata_equal(geoslave,masterdata,data):
                    return  [True,"Succeed to synchronize the changes of the wmslayer({1}.{2}.{3}) to geocluster slave server({0})".format(geoslave.geoserver_url,wsname,storename,layername)]
                else:
                    geoslave.reload()
                    return  [False,"Failed to synchronize the changes of the wmsstore({1}.{2}.{3}) to geocluster slave server({0}),\n    master data = {4}\n    slave data = {5}".format(geoslave.geoserver_url,wsname,storename,layername,json.dumps(masterdata,indent=4),json.dumps(data,indent=4))]
            if (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
                time.sleep(self.check_interval)
            else:
                break

        geoslave.reload()

        try:
            data1 = geoslave.get_wmslayer(wsname,layername,storename=storename)
        except ResourceNotFound as ex:
            data1 = None
        if not data1 or not self.is_metadata_equal(geoslave,masterdata,data1):
            raise Exception("post_publish_wmslayer: Failed to reload the data for geocluster slave server({})".format(geoslave.geoserver_url))

        return  [False,"Failed to synchronize the changes of the wmsstore({1}.{2}.{3}) to geocluster slave server({0})".format(geoslave.geoserver_url,wsname,storename,layername)]

    def post_update_wmslayer(self,wsname,storename,layername,parameters):
        return self._check_geoslave(self._post_update_wmslayer,wsname,storename,layername,parameters)

    def _post_update_wmslayer(self,geoslave,starttime,wsname,storename,layername,parameters):
        masterdata = None
        data =  None
        masterdata = self.geoserver.get_wmslayer(wsname,layername,storename=storename)
        while True:
            try:
                data = geoslave.get_wmslayer(wsname,layername,storename=storename)
            except ResourceNotFound as ex:
                pass
            if data:
                if self.is_metadata_equal(geoslave,masterdata,data):
                    return  [True,"Succeed to synchronize the changes of the wmslayer({1}.{2}.{3}) to geocluster slave server({0})".format(geoslave.geoserver_url,wsname,storename,layername)]
            if (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
                time.sleep(self.check_interval)
            else:
                break

        geoslave.reload()

        try:
            data1 = geoslave.get_wmslayer(wsname,layername,storename=storename)
        except ResourceNotFound as ex:
            data1 = None
        if not data1 or not self.is_metadata_equal(geoslave,masterdata,data1):
            raise Exception("post_update_wmslayer: Failed to reload the data for geocluster slave server({})".format(geoslave.geoserver_url))

        return  [False,"Failed to synchronize the changes of the wmsstore({1}.{2}.{3}) to geocluster slave server({0}),\n    master data = {4}\n    slave data = {5}".format(geoslave.geoserver_url,wsname,storename,layername,json.dumps(masterdata,indent=4),json.dumps(data,indent=4))]

    
    def post_delete_wmslayer(self,wsname,storename,layername):
        return self._check_geoslave(self._post_delete_wmslayer,wsname,storename,layername)

    def _post_delete_wmslayer(self,geoslave,starttime,wsname,storename,layername):
        while True:
            if not geoslave.has_wmslayer(wsname,layername,storename=storename):
                return  [True,"Succeed to synchronize the deletion of the wmslayer({1}.{2}.{3}) to geocluster slave server({0})".format(geoslave.geoserver_url,wsname,storename,layername)]
            elif (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
                time.sleep(self.check_interval)
            else:
                break

        geoslave.reload()

        if geoslave.has_wmslayer(wsname,layername,storename=storename):
            raise Exception("post_delete_wmslayer: Failed to reload the data for geocluster slave server({})".format(geoslave.geoserver_url))

        return  [False,"Failed to synchronize the deletion of the wmsstore({1}.{2}.{3}) to geocluster slave server({0})".format(geoslave.geoserver_url,wsname,storename,layername)]

    def post_create_layergroup(self,wsname,groupname,parameters):
        return self._check_geoslave(self._post_create_layergroup,wsname,groupname,parameters)

    def _post_create_layergroup(self,geoslave,starttime,wsname,groupname,parameters):
        masterdata = self.geoserver.get_layergroup(wsname,groupname)
        data = None
        while True:
            try:
                data = geoslave.get_layergroup(wsname,groupname)
            except ResourceNotFound as ex:
                pass
            if data:
                if self.is_metadata_equal(geoslave,masterdata,data):
                    return  [True,"Succeed to synchronize the changes of the layergroup({1}.{2}) to geocluster slave server({0})".format(geoslave.geoserver_url,wsname,groupname)]
                else:
                    geoslave.reload()
                    return  [False,"Failed to synchronize the changes of the layergroup({1}.{2}) to geocluster slave server({0}),\n    master data = {3}\n    slave data = {4}".format(geoslave.geoserver_url,wsname,groupname,json.dumps(masterdata,indent=4),json.dumps(data,indent=4))]
            if (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
                time.sleep(self.check_interval)
            else:
                break

        geoslave.reload()

        try:
            data1 = geoslave.get_layergroup(wsname,groupname)
        except ResourceNotFound as ex:
            data1 = None
        if not data1 or not self.is_metadata_equal(geoslave,masterdata,data1):
            raise Exception("post_create_layergroup: Failed to reload the data for geocluster slave server({})".format(geoslave.geoserver_url))

        return  [False,"Failed to synchronize the changes of the layergroup({1}.{2}) to geocluster slave server({0})".format(geoslave.geoserver_url,wsname,groupname)]

    def post_update_layergroup(self,wsname,groupname,parameters):
        return self._check_geoslave(self._post_update_layergroup,wsname,groupname,parameters)

    def _post_update_layergroup(self,geoslave,starttime,wsname,groupname,parameters):
        masterdata = None
        data = None
        masterdata = self.geoserver.get_layergroup(wsname,groupname)
        while True:
            try:
                data = geoslave.get_layergroup(wsname,groupname)
            except ResourceNotFound as ex:
                pass
            if data:
                if self.is_metadata_equal(geoslave,masterdata,data):
                    return  [True,"Succeed to synchronize the changes of the layergroup({1}.{2}) to geocluster slave server({0})".format(geoslave.geoserver_url,wsname,groupname)]
            if (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
                time.sleep(self.check_interval)
            else:
                break

        geoslave.reload()

        try:
            data1 = geoslave.get_layergroup(wsname,groupname)
        except ResourceNotFound as ex:
            data1 = None
        if not data1 or not self.is_metadata_equal(geoslave,masterdata,data1):
            raise Exception("post_update_layergroup: Failed to reload the data for geocluster slave server({})".format(geoslave.geoserver_url))

        return  [False,"Failed to synchronize the changes of the layergroup({1}.{2}) to geocluster slave server({0}),\n    master data = {3}\n    slave data = {4}".format(geoslave.geoserver_url,wsname,groupname,json.dumps(masterdata,indent=4),json.dumps(data,indent=4))]

    def post_delete_layergroup(self,wsname,groupname):
        return self._check_geoslave(self._post_delete_layergroup,wsname,groupname)

    def _post_delete_layergroup(self,geoslave,starttime,wsname,groupname):
        while True:
            if not geoslave.has_layergroup(wsname,groupname):
                return  [True,"Succeed to synchronize the deletion of the layergroup({1}.{2}) to geocluster slave server({0})".format(geoslave.geoserver_url,wsname,groupname)]
            elif (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
                time.sleep(self.check_interval)
            else:
                break

        geoslave.reload()

        if geoslave.has_layergroup(wsname,groupname):
            raise Exception("post_delete_layergroup: Failed to reload the data for geocluster slave server({})".format(geoslave.geoserver_url))

        return  [False,"Failed to synchronize the deletion of the layergroup({1}.{2}) to geocluster slave server({0})".format(geoslave.geoserver_url,wsname,groupname)]

    def post_create_wmtslayer(self,wsname,layername,parameters):
        return self._check_geoslave(self._post_create_wmtslayer,wsname,layername,parameters)

    def _post_create_wmtslayer(self,geoslave,starttime,wsname,layername,parameters):
        masterdata = self.geoserver.get_gwclayer(wsname,layername)
        data = None
        while True:
            try:
                data = geoslave.get_gwclayer(wsname,layername)
            except ResourceNotFound as ex:
                pass
            if data:
                if self.is_metadata_equal(geoslave,masterdata,data):
                    return  [True,"Succeed to synchronize the changes of the gwclayer({1}.{2}) to geocluster slave server({0})".format(geoslave.geoserver_url,wsname,layername)]
                else:
                    geoslave.reload()
                    return  [False,"Failed to synchronize the changes of the gwclayer({1}.{2}) to geocluster slave server({0}),\n    master data = {3}\n    slave data = {4}".format(geoslave.geoserver_url,wsname,layername,json.dumps(masterdata,indent=4),json.dumps(data,indent=4))]
            if (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
                time.sleep(self.check_interval)
            else:
                break

        geoslave.reload()

        try:
            data1 = geoslave.get_gwclayer(wsname,layername)
        except ResourceNotFound as ex:
            data1 = None
        if not data1 or not self.is_metadata_equal(geoslave,masterdata,data1):
            raise Exception("post_create_wmtslayer: Failed to reload the data for geocluster slave server({})".format(geoslave.geoserver_url))

        return  [False,"Failed to synchronize the changes of the gwclayer({1}.{2}) to geocluster slave server({0})".format(geoslave.geoserver_url,wsname,layername)]

    def post_update_wmtslayer(self,wsname,layername,parameters):
        return self._check_geoslave(self._post_update_wmtslayer,wsname,layername,parameters)

    def _post_update_wmtslayer(self,geoslave,starttime,wsname,layername,parameters):
        masterdata = None
        data = None
        masterdata = self.geoserver.get_gwclayer(wsname,layername)
        while True:
            try:
                data = geoslave.get_gwclayer(wsname,layername)
            except ResourceNotFound as ex:
                pass
            if data:
                if self.is_metadata_equal(geoslave,masterdata,data):
                    return  [True,"Succeed to synchronize the changes of the gwclayer({1}.{2}) to geocluster slave server({0})".format(geoslave.geoserver_url,wsname,layername)]
            if (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
                time.sleep(self.check_interval)
            else:
                break

        geoslave.reload()

        try:
            data1 = geoslave.get_gwclayer(wsname,layername)
        except ResourceNotFound as ex:
            data1 = None
        if not data1 or not self.is_metadata_equal(geoslave,masterdata,data1):
            raise Exception("post_update_wmtslayer: Failed to reload the data for geocluster slave server({})".format(geoslave.geoserver_url))

        return  [False,"Failed to synchronize the changes of the gwclayer({1}.{2}) to geocluster slave server({0}),\n    master data = {3}\n    slave data = {4}".format(geoslave.geoserver_url,wsname,layername,json.dumps(masterdata,indent=4),json.dumps(data,indent=4))]
        
    def post_delete_wmtslayer(self,wsname,layername):
        return self._check_geoslave(self._post_delete_wmtslayer,wsname,layername)

    def _post_delete_wmtslayer(self,geoslave,starttime,wsname,layername):
        while True:
            if not geoslave.has_gwclayer(wsname,layername):
                return  [True,"Succeed to synchronize the deletion of the gwclayer({1}.{2}) to geocluster slave server({0})".format(geoslave.geoserver_url,wsname,layername)]
            elif (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
                time.sleep(self.check_interval)
            else:
                break

        geoslave.reload()

        if geoslave.has_gwclayer(wsname,layername):
            raise Exception("post_delete_wmtslayer: Failed to reload the data for geocluster slave server({})".format(geoslave.geoserver_url))

        return  [False,"Failed to synchronize the deletion of the gwclayer({1}.{2}) to geocluster slave server({0})".format(geoslave.geoserver_url,wsname,layername)]

    def post_get_original_tile(self,wsname,layername,tile):
        return self._check_geoslave(self._post_get_original_tile,wsname,layername,tile)

    def _post_get_original_tile(self,geoslave,starttime,wsname,layername,tile):
        slavetile = None
        try:
            slavetile = geoslave.get_tile(wsname,layername)
            if filecmp.cmp(slavetile,tile):
                return  [True,"The tile of the layer({1}.{2}) in geocluster slave server({0}) matches the tile in master server".format(geoslave.geoserver_url,wsname,layername)]
            else:
                return  [False,"The tile of the layer({1}.{2}) in geocluster slave server({0}) doesn't match the tile in master server".format(geoslave.geoserver_url,wsname,layername)]
        finally:
            utils.remove_file(slavetile)


    def post_get_tile_beforeexpire(self,wsname,layername,tile):
        return self._check_geoslave(self._post_get_tile_beforeexpire,wsname,layername,tile)

    def _post_get_tile_beforeexpire(self,geoslave,starttime,wsname,layername,tile):
        slavetile = None
        try:
            slavetile = geoslave.get_tile(wsname,layername)
            if filecmp.cmp(slavetile,tile):
                return  [True,"The cached tile of the layer({1}.{2}) in geocluster slave server({0}) matches the tile in master server".format(geoslave.geoserver_url,wsname,layername)]
            else:
                return  [False,"The cached tile of the layer({1}.{2}) in geocluster slave server({0}) doesn't match the tile in master server".format(geoslave.geoserver_url,wsname,layername)]
        finally:
            utils.remove_file(slavetile)

    def post_gwccache_expire(self,wsname,layername,tile):
        return self._check_geoslave(self._post_gwccache_expire,wsname,layername,tile)

    def _post_gwccache_expire(self,geoslave,starttime,wsname,layername,tile):
        slavetile = None
        try:
            slavetile = geoslave.get_tile(wsname,layername)
            if filecmp.cmp(slavetile,tile):
                return  [True,"The expired and regenerated tile of the layer({1}.{2}) in geocluster slave server({0}) matches the tile in master server".format(geoslave.geoserver_url,wsname,layername)]
            else:
                return  [False,"The expired and regenerated tile of the layer({1}.{2}) in geocluster slave server({0}) doesn't match the tile in master server".format(geoslave.geoserver_url,wsname,layername)]
        finally:
            utils.remove_file(slavetile)

    def post_get_tile_beforeclear(self,wsname,layername,tile):
        return self._check_geoslave(self._post_get_tile_beforeclear,wsname,layername,tile)

    def _post_get_tile_beforeclear(self,geoslave,starttime,wsname,layername,tile):
        slavetile = None
        try:
            slavetile = geoslave.get_tile(wsname,layername)
            if filecmp.cmp(slavetile,tile):
                return  [True,"The cached tile of the layer({1}.{2}) in geocluster slave server({0}) matches the tile in master server".format(geoslave.geoserver_url,wsname,layername)]
            else:
                return  [False,"The cached tile of the layer({1}.{2}) in geocluster slave server({0}) doesn't match the tile in master server".format(geoslave.geoserver_url,wsname,layername)]
        finally:
            utils.remove_file(slavetile)

    def post_empty_gwccache1(self,wsname,layername,tile):
        return self._check_geoslave(self._post_empty_gwccache,wsname,layername,tile)

    def _post_empty_gwccache(self,geoslave,starttime,wsname,layername,tile):
        slavetile = None
        while True:
            try:
                slavetile = geoslave.get_tile(wsname,layername)
                if filecmp.cmp(slavetile,tile):
                    return  [True,"The cleared and regenerated tile of the layer({1}.{2}) in geocluster slave server({0}) matches the tile in master server".format(geoslave.geoserver_url,wsname,layername)]
                elif (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
                    time.sleep(self.check_interval)
                else:
                    break
            finally:
                utils.remove_file(slavetile)
                slavetile = None


        geoslave.empty_gwclayer(wsname,layername)
        try:
            slavetile = geoslave.get_tile(wsname,layername)
            if not filecmp.cmp(slavetile,tile):
                raise Exception("post_empty_gwccache: Failed to empty gwc cache for geocluster slave server({})".format(geoslave.geoserver_url))
        finally:
            utils.remove_file(slavetile)

        return  [False,"The cleared and regenerated tile of the layer({1}.{2}) in geocluster slave server({0}) doesn't match the tile in master server".format(geoslave.geoserver_url,wsname,layername)]


    def post_create_usergroup(self,groupname):
        return self._check_geoslave(self._post_create_usergroup,groupname)

    def _post_create_usergroup(self,geoslave,starttime,groupname):
        starttime = datetime.now()
        while True:
            if geoslave.has_usergroup(groupname):
                return  [True,"Succeed to synchronize the creation of the usergroup({1}) to geocluster slave server({0})".format(geoslave.geoserver_url,groupname)]
            elif (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
                time.sleep(self.check_interval)
            else:
                break

        geoslave.reload()

        if not geoslave.has_usergroup(groupname):
            raise Exception("post_create_usergroup: Failed to reload the data for geocluster slave server({})".format(geoslave.geoserver_url))

        return  [False,"Failed to synchronize the creation of the usergroup({1}) to geocluster slave server({0})".format(geoslave.geoserver_url,groupname)]

    def post_delete_usergroup(self,groupname):
        return self._check_geoslave(self._post_delete_usergroup,groupname)

    def _post_delete_usergroup(self,geoslave,starttime,groupname):
        starttime = datetime.now()
        while True:
            if not geoslave.has_usergroup(groupname):
                return  [True,"Succeed to synchronize the deletion of the usergroup({1}) to geocluster slave server({0})".format(geoslave.geoserver_url,groupname)]
            elif (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
                time.sleep(self.check_interval)
            else:
                break

        geoslave.reload()

        if geoslave.has_usergroup(groupname):
            raise Exception("post_delete_usergroup: Failed to reload the data for geocluster slave server({})".format(geoslave.geoserver_url))

        return  [False,"Failed to synchronize the deletion of the usergroup({1}) to geocluster slave server({0})".format(geoslave.geoserver_url,groupname)]

    def post_create_user(self,user,password,enable):
        return self._check_geoslave(self._post_create_user,user,password,enable)

    def _post_create_user(self,geoslave,starttime,user,password,enable):
        result = None
        while True:
            data = None
            try:
                data = geoslave.get_user(user)
            except ObjectNotFound as ex:
                pass
            if data:
                if data[1] != enable:
                    result =  [False,"Failed to synchronize the enable status({2}) of the user({1}) to geocluster slave server({0})".format(geoslave.geoserver_url,user,enable)]
                    break
                if not geoslave.login(user,password):
                    result = [False,"Failed to synchronize the password of the user({1}) to geocluster slave server({0})".format(geoslave.geoserver_url,user)]
                    break
                return  [True,"Succeed to synchronize the changes of the user({1}) to geocluster slave server({0})".format(geoslave.geoserver_url,user)]
            if (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
                time.sleep(self.check_interval)
            else:
                break


        geoslave.reload()

        try:
            data1 = geoslave.get_user(user)
        except ResourceNotFound as ex:
            data1 = None
        if not data1 or data1[1] != enable or not geoslave.login(user,password):
            raise Exception("post_create_user: Failed to reload the data for geocluster slave server({})".format(geoslave.geoserver_url))

        return  result or [False,"Failed to synchronize the changes of the user({1}) to geocluster slave server({0})".format(geoslave.geoserver_url,user)]

    def post_enable_user(self,user,enable):
        return self._check_geoslave(self._post_enable_user,user,enable)

    def _post_enable_user(self,geoslave,starttime,user,enable):
        while True:
            data = None
            try:
                data = geoslave.get_user(user)
            except ObjectNotFound as ex:
                pass
            if data:
                if data[1] == enable:
                    return  [True,"Succeed to synchronize the enable status({2}) of the user({1}) to geocluster slave server({0})".format(geoslave.geoserver_url,user,enable)]
            if (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
                time.sleep(self.check_interval)
            else:
                break

        geoslave.reload()

        try:
            data1 = geoslave.get_user(user)
        except ResourceNotFound as ex:
            data1 = None
        if not data1 or data1[1] != enable :
            raise Exception("post_enable_user: Failed to reload the data for geocluster slave server({})".format(geoslave.geoserver_url))

        return  [False,"Failed to synchronize the enable status({2}) of the user({1}) to geocluster slave server({0})".format(geoslave.geoserver_url,user,enable)]

    def post_change_userpassword(self,user,oldpassword,newpassword):
        return self._check_geoslave(self._post_change_userpassword,user,oldpassword,newpassword)

    def _post_change_userpassword(self,geoslave,starttime,user,oldpassword,newpassword):
        while True:
            if geoslave.login(user,newpassword) :
                return  [True,"Succeed to synchronize the changes of the user({1}) to geocluster slave server({0})".format(geoslave.geoserver_url,user)]
            elif (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
                time.sleep(self.check_interval)
            else:
                break
                
        geoslave.reload()

        try:
            data1 = geoslave.get_user(user)
        except ResourceNotFound as ex:
            data1 = None
        if not data1 or not geoslave.login(user,newpassword) :
            raise Exception("post_change_userpassword: Failed to reload the data for geocluster slave server({})".format(geoslave.geoserver_url))

        return  [False,"Failed to synchronize the password of the user({1}) to geocluster slave server({0})".format(geoslave.geoserver_url,user)]

    def post_delete_user(self,user):
        return self._check_geoslave(self._post_delete_user,user)

    def _post_delete_user(self,geoslave,starttime,user):
        while True:
            if not geoslave.has_user(user):
                return  [True,"Succeed to synchronize the deletion of the user({1}) to geocluster slave server({0})".format(geoslave.geoserver_url,user)]
            elif (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
                time.sleep(self.check_interval)
            else:
                break

        geoslave.reload()

        if geoslave.has_user(user):
            raise Exception("post_delete_user: Failed to reload the data for geocluster slave server({})".format(geoslave.geoserver_url))

        return  [False,"Failed to synchronize the deletion of the user({1}) to geocluster slave server({0})".format(geoslave.geoserver_url,user)]

    def post_add_user_to_group(self,user,group):
        return self._check_geoslave(self._post_add_user_to_group,user,group)

    def _post_add_user_to_group(self,geoslave,starttime,user,group):
        starttime = datetime.now()
        while True:
            if geoslave.user_in_group(user,group):
                return  [True,"Succeed to synchronize the operation of adding the user({1}) to usergroup({2}) to geocluster slave server({0})".format(geoslave.geoserver_url,user,group)]
            elif (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
                time.sleep(self.check_interval)
            else:
                break

        geoslave.reload()

        if not geoslave.user_in_group(user,group):
            raise Exception("post_add_user_to_group: Failed to reload the data for geocluster slave server({})".format(geoslave.geoserver_url))

        return  [False,"Failed to synchronize the operation of adding the user({1}) to usergroup({2}) to geocluster slave server({0})".format(geoslave.geoserver_url,user,group)]

    def post_delete_user_from_group(self,user,group):
        return self._check_geoslave(self._post_delete_user_from_group,user,group)

    def _post_delete_user_from_group(self,geoslave,starttime,user,group):
        while True:
            if not geoslave.user_in_group(user,group):
                return  [True,"Succeed to synchronize the operation of deleting the user({1}) from usergroup({2}) to geocluster slave server({0})".format(geoslave.geoserver_url,user,group)]
            elif (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
                time.sleep(self.check_interval)
            else:
                break

        geoslave.reload()

        if geoslave.user_in_group(user,group):
            raise Exception("post_delete_user_from_group: Failed to reload the data for geocluster slave server({})".format(geoslave.geoserver_url))

        return  [False,"Failed to synchronize the operation of deleting the user({1}) from usergroup({2}) to geocluster slave server({0})".format(geoslave.geoserver_url,user,group)]

    def post_create_role(self,role):
        return self._check_geoslave(self._post_create_role,role)

    def _post_create_role(self,geoslave,starttime,role):
        while True:
            if geoslave.has_role(role):
                return  [True,"Succeed to synchronize the creation of the role({1}) to geocluster slave server({0})".format(geoslave.geoserver_url,role)]
            elif (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
                time.sleep(self.check_interval)
            else:
                break

        geoslave.reload()

        if not geoslave.has_role(role):
            raise Exception("post_create_role: Failed to reload the data for geocluster slave server({})".format(geoslave.geoserver_url))

        return  [False,"Failed to synchronize the creation of the role({1}) to geocluster slave server({0})".format(geoslave.geoserver_url,role)]

    def post_delete_role(self,role):
        return self._check_geoslave(self._post_delete_role,role)

    def _post_delete_role(self,geoslave,starttime,role):
        while True:
            if not geoslave.has_role(role):
                return  [True,"Succeed to synchronize the deletion of the role({1}) to geocluster slave server({0})".format(geoslave.geoserver_url,role)]
            elif (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
                time.sleep(self.check_interval)
            else:
                break

        geoslave.reload()

        if geoslave.has_role(role):
            raise Exception("post_delete_role: Failed to reload the data for geocluster slave server({})".format(geoslave.geoserver_url))

        return  [False,"Failed to synchronize the deletion of the role({1}) to geocluster slave server({0})".format(geoslave.geoserver_url,role)]

    def post_associate_role_with_user(self,role,user):
        return self._check_geoslave(self._post_associate_role_with_user,role,user)

    def _post_associate_role_with_user(self,geoslave,starttime,role,user):
        while True:
            if geoslave.user_has_role(user,role):
                return  [True,"Succeed to synchronize the operation of associating the user({2}) with the role({1}) to geocluster slave server({0})".format(geoslave.geoserver_url,role,user)]
            elif (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
                time.sleep(self.check_interval)
            else:
                break

        geoslave.reload()

        if not geoslave.user_has_role(user,role):
            raise Exception("post_associate_role_with_user: Failed to reload the data for geocluster slave server({})".format(geoslave.geoserver_url))

        return  [False,"Failed to synchronize the operation of associating the user({2}) with the role({1}) to geocluster slave server({0})".format(geoslave.geoserver_url,role,user)]

    def post_unassociate_role_with_user(self,role,user):
        return self._check_geoslave(self._post_unassociate_role_with_user,role,user)

    def _post_unassociate_role_with_user(self,geoslave,starttime,role,user):
        while True:
            if not geoslave.user_has_role(user,role):
                return  [True,"Succeed to synchronize the operation of unassociating the user({2}) with the role({1}) to geocluster slave server({0})".format(geoslave.geoserver_url,role,user)]
            elif (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
                time.sleep(self.check_interval)
            else:
                break

        geoslave.reload()

        if geoslave.user_has_role(user,role):
            raise Exception("post_unassociate_role_with_user: Failed to reload the data for geocluster slave server({})".format(geoslave.geoserver_url))

        return  [False,"Failed to synchronize the operation of unassociating the user({2}) with the role({1}) to geocluster slave server({0})".format(geoslave.geoserver_url,role,user)]

    def post_associate_role_with_usergroup(self,role,group):
        return self._check_geoslave(self._post_associate_role_with_usergroup,role,group)

    def _post_associate_role_with_usergroup(self,geoslave,starttime,role,group):
        while True:
            if geoslave.usergroup_has_role(group,role):
                return  [True,"Succeed to synchronize the operation of associating the usergroup({2}) with the role({1}) to geocluster slave server({0})".format(geoslave.geoserver_url,role,group)]
            elif (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
                time.sleep(self.check_interval)
            else:
                break

        geoslave.reload()

        if not geoslave.usergroup_has_role(group,role):
            raise Exception("post_associate_role_with_usergroup: Failed to reload the data for geocluster slave server({})".format(geoslave.geoserver_url))

        return  [False,"Failed to synchronize the operation of associating the usergroup({2}) with the role({1}) to geocluster slave server({0})".format(geoslave.geoserver_url,role,group)]

    def post_unassociate_role_with_usergroup(self,role,group):
        return self._check_geoslave(self._post_unassociate_role_with_usergroup,role,group)

    def _post_unassociate_role_with_usergroup(self,geoslave,starttime,role,group):
        while True:
            if not geoslave.usergroup_has_role(group,role):
                return  [True,"Succeed to synchronize the operation of unassociating the usergroup({2}) with the role({1}) to geocluster slave server({0})".format(geoslave.geoserver_url,role,group)]
            elif (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
                time.sleep(self.check_interval)
            else:
                break

        geoslave.reload()

        if geoslave.usergroup_has_role(group,role):
            raise Exception("post_unassociate_role_with_usergroup: Failed to reload the data for geocluster slave server({})".format(geoslave.geoserver_url))

        return  [False,"Failed to synchronize the operation of unassociating the usergroup({2}) with the role({1}) to geocluster slave server({0})".format(geoslave.geoserver_url,role,group)]

    def post_grant_layer_access_permission(self,access_rules):
        return self._check_geoslave(self._post_grant_layer_access_permission,access_rules)

    def _post_grant_layer_access_permission(self,geoslave,starttime,access_rules):
        data = None
        masterdata = None
        masterdata = self.geoserver.get_layer_access_rules()
        while True:
            data = geoslave.get_layer_access_rules()
            if data:
                if self.is_metadata_equal(geoslave,masterdata,data):
                    return  [True,"Succeed to synchronize the changes of the layer access permissions to geocluster slave server({0})".format(geoslave.geoserver_url)]
            if (datetime.now() - starttime).total_seconds() <= self.sync_timeout:
                time.sleep(self.check_interval)
            else:
                break

        geoslave.reload()

        try:
            data1 = geoslave.get_layer_access_rules()
        except ResourceNotFound as ex:
            data1 = None
        if not data1 or not self.is_metadata_equal(geoslave,masterdata,data1):
            raise Exception("post_grant_layer_access_permission: Failed to reload the data for geocluster slave server({})".format(geoslave.geoserver_url))

        return  [False,"Failed to synchronize the changes of the layer access permissions to geocluster slave server({0}),\n    master data = {1}\n    slave data = {2}".format(geoslave.geoserver_url,json.dumps(masterdata,indent=4),json.dumps(data,indent=4))]
        
    def post_revoke_layer_access_permission(self,delete_permissions):
        return self.post_grant_layer_access_permission(delete_permissions)

    def post_get_wfs_capabilities(self,master_capabilities_xmlfile):
        return self._check_geoslave(self._post_get_wfs_capabilities,master_capabilities_xmlfile)

    def _post_get_wfs_capabilities(self,geoslave,starttime,master_capabilities_xmlfile):
        try:
            file = "/tmp/wfscapabilities_{}_{}.xml".format(self.sufix,hash(geoslave.geoserver_url))
            geoslave.get_wfscapabilities(outputfile=file)
            return [True,"Succeed to get the wfs capabilities"]
        except Exception as ex:
            return [False,"Failed to get the wfs capabilities.{}".format(ex)]
        finally:
            utils.remove_file(file)
        
    def post_get_wms_capabilities(self,master_capabilities_xmlfile):
        return self._check_geoslave(self._post_get_wms_capabilities,master_capabilities_xmlfile)

    def _post_get_wms_capabilities(self,geoslave,starttime,master_capabilities_xmlfile):
        try:
            file = "/tmp/wmscapabilities_{}_{}.xml".format(self.sufix,hash(geoslave.geoserver_url))
            geoslave.get_wmscapabilities(outputfile=file)
            return [True,"Succeed to get the wms capabilities"]
        except Exception as ex:
            return [False,"Failed to get the wms capabilities.{}".format(ex)]
        finally:
            utils.remove_file(file)
        
    def post_get_wmts_capabilities(self,master_capabilities_xmlfile):
        return self._check_geoslave(self._post_get_wmts_capabilities,master_capabilities_xmlfile)

    def _post_get_wmts_capabilities(self,geoslave,starttime,master_capabilities_xmlfile):
        try:
            file = "/tmp/wmtscapabilities_{}_{}.xml".format(self.sufix,hash(geoslave.geoserver_url))
            geoslave.get_wmtscapabilities(outputfile=file)
            return [True,"Succeed to get the wmts capabilities"]
        except Exception as ex:
            return [False,"Failed to get the wmts capabilities.{}".format(ex)]
        finally:
            utils.remove_file(file)
        

if __name__ == '__main__':
    geoserver_url = os.environ["GEOSERVER_URL"]
    geoslaves_url = os.environ["GEOSLAVES_URL"]
    geoserver_user = os.environ.get("GEOSERVER_USER")
    geoserver_password = os.environ.get("GEOSERVER_PASSWORD")
    geoserver_ssl_verify = os.environ.get("GEOSERVER_SSL_VERIFY","True").lower() == "true"
    compatibilitycheck = GeoclusterCompatibilityCheck(geoserver_url,geoslaves_url,geoserver_user,geoserver_password,settings.GET_REQUEST_HEADERS("GEOSERVER_REQUEST_HEADERS"),ssl_verify=geoserver_ssl_verify)
    compatibilitycheck.run()


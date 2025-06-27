import os
import filecmp
import re
import queue
import threading
import logging
import shutil
import jinja2
import traceback
from collections import OrderedDict

from .taskrunner import TaskRunner
from .geoserver import Geoserver
from .tasks import *
from .csv import CSVWriter
from . import settings
from . import timezone
from . import loggingconfig
from . import utils
from .mail import EmailMessage
from . import utils

logger = logging.getLogger("geoserver_rest.geoserverhealthcheck")

jinja_env = jinja2.Environment(
    loader=jinja2.FileSystemLoader([settings.BASE_DIR]),
    autoescape=jinja2.select_autoescape()
)

class GeoserverCompatibilityCheck(object):
    report_file = None
    warnings_file = None
    reportwriter = None
    warningwriter = None
    _finished_tasks = None
    metadata = None
    
    def __init__(self,geoserver_url,geoserver_user,geoserver_password,requestheaders=None):
        utils.init_logging()
        self.geoserver = Geoserver(geoserver_url,geoserver_user,geoserver_password,headers=requestheaders)
        self._resources = OrderedDict()
        self._checklist = OrderedDict()

    def run(self):
        pass

    @property
    def style_folder(self):
        """
        Return style folder; return None if not configured properly

        """
        folder = os.environ.get("STYLE_FOLDER")
        if folder:
            return folder
        sample_dataset = os.environ.get("SAMPLE_DATASET")
        if sample_dataset:
            return os.path.split(sample_dataset)[0]
        return None

    def _update_checklist(self,resource,operation,status):
        """
        Update the check list
        update the status and append the description
        resource: the resource which is checked on
        operation: 
        status. a tuple(status: True/False, Desc)
        """
        if not status:
            return

        if isinstance(status[0],list):
            for s  in status:
                self._update_checklist(resource,operation,s)
        else:
            if resource not in self._checklist:
                self._checklist[resource] = OrderedDict()
    
            if operation in self._checklist[resource]:
                self._checklist[resource][operation][0] = self._checklist[resource][operation][0] and status[0]
                self._checklist[resource][operation][1].append(status)
            else:
                self._checklist[resource][operation] = [status[0],[status]]

    def _add_container_resource(self,*keys,parameters=None):
        resouces = self._resources
        for key in keys:
            if key not in resources:
                resources[key] = OrderedDict()
            resources = resources[key]

        if parameters:
            resources["__parameters__"] = parameters
            
    def _set_resource(self,value,*keys):
        resouces = self._resources
        for key in keys[:-1]:
            if key not in resources:
                resources[key] = OrderedDict()
            resources = resources[key]

        resources[keys[-1]] = value
            
    def _append_resource(self,value,*keys):
        resouces = self._resources
        for key in keys[:-1]:
            if key not in resources:
                resources[key] = OrderedDict()
            resources = resources[key]

        if keys[-1] in resources:
            resources[keys[-1]].append(value)
        else:
            resources[keys[-1]] = [value]

    resourcesufix = "compatibilitycheck"

    def reset_env(self):
        for wsname in self.geoserver.list_workspaces():
            if not wsname.endswith(self.resourcesufix):
                continue
            self.geoserver.delete_workspace(wsname,recurse=True)
            if self.geoserver.has_workspace(wsname):
                raise Exception("Failed to delete workspace '{}'".format(wsname)]))

        for usergroup in self.list_usergroups():
            if not usergroup.endswith(self.resourcesufix):
                continue

            self.geoserver.delete_usergroup(usergroup)
            if self.geoserver.has_usergroup(usergroup):
                raise Exception("Failed to delete user group '{}'".format(usergroup)]))

        for user,userenabled in self.list_users():
            if not user.endwith(self.resourcesufix):
                continue

            self.geoserver.delete_user(user)
            if self.geoserver.has_user(user):
                raise Exception("Failed to delete user '{}'".format(user)]))

    def _create_workspace(self,wsname):
        """
        Create workspace,
        Return True if succeed, otherwise return False
        """
        try:
            self.geoserver.create_workspace(wsname)
            if self.geoserver.has_workspace(wsname):
                self._update_checklist("workspace","create",[True,"Succeed to create the workspace '{}'".format(wsname)])
                self._add_container_resource("workspaces",wsname)
                self._update_checklist("workspace","create",self.post_create_workspace(wsname))
                return True
            else:
                self._update_checklist("workspace","create" ,[False,"Failed to create workspace '{}'".format(wsname)])
                return False
        except Exception as ex:
            self._update_checklist("workspace","create" ,[False,"Failed to create workspace '{}'. {}".format(wsname,ex)])
            return False

    def post_create_workspace(self,wsname):
        """
        called after workspace creation.
        """
        return None

    def delete_workspace(self):
        for wsname in self._resources.get("workspaces",{}).keys():
            if wsname == "__parameters__":
                continue
            try:
                self.geoserver.delete_workspace(wsname)
                if self.geoserver.has_workspace(wsname):
                    self._update_checklist("workspace","delete",[False,"Failed to delete workspace '{}'".format(wsname)])
                    self._update_checklist("workspace","delete",self.post_delete_workspace(wsname))
                else:
                    self._update_checklist("workspace","delete",[True,"Succeed to delete workspace '{}'".format(wsname)])
            except Exception as ex:
                self._update_checklist("workspace","delete",[False,"Failed to delete workspace '{}'.{}".format(wsname,ex)])

    def post_delete_workspace(self,wsname):
        """
        called after workspace delete.
        """
        return None

    def create_localdatastore(self,wsname):
        """
        create the test workspace if doesn't have
        Return the create (datastore , dataset file). Return None if local dataset is not configured, or create failed
        """
        #create the test workspace if doesn't have
        dataset = os.environ.get("SAMPLE_DATASET")
        if not dataset:
            return None

        storename = "localds4compatibilitycheck"
        operation = "create local datastore"
        try:
            self.geoserver.upload_dataset(wsname,storename,dataset)
            if self.geoserver.has_datastore(wsname,storename):
                self._add_container_resource("workspaces",wsname,"datastores",storename,parameters=dataset)
                self._update_checklist("datastore",operation,[True,"Succeed to create the datastore '{}.{}' from local dataset({})".format(wsname,storename,dataset)])
                self._update_checklist("datastore",operation,self.post_create_localdatastore(wsname,storename,dataset))
                return (storename,dataset)
            else:
                self._update_checklist("datastore",operation,[False,"Failed to create the datastore '{}.{}' from local dataset({})".format(wsname,storename,dataset)])
                return None
        except Exception as ex:
            self._update_checklist("datastore",operation,[False,"Failed to create the datastore '{}.{}' from local dataset({}). {}".format(wsname,storename,dataset,ex)])
            return None

    def post_create_localdatastore(self,wsname,storename,dataset):
        """
        Called after datastore creation
        """
        return None

    def create_postgisdatastore(self,wsname):
        """
        create the test workspace if doesn't have
        Return the created datastore. Return None if postgis is not configured, or create failed
        """
        if any(False if os.environ.get(key) else True for key in ("POSTGIS_HOST","POSTGIS_PORT","POSTGIS_DATABASE","POSTGIS_USER")):
            #test postgis datastore disabled
            return None

        storename = "postgisds4compatibilitycheck"
        operation = "create postgis datastore"
        try:
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
            self.geoserver.update_datastore(wsname,storename,parameters,create=True)
            if self.geoserver.has_datastore(wsname,storename):
                storedata = self.geoserver.get_datastore(wsname,storename)
                for k,v in parameters.items():
                    if k == "passwd":
                        continue
                    if self.geoserver.get_datastore_field(storedata,k) != str(v):
                        raise Exception("The field({1}) of the datastore({0}) should be {3} instead of {2}".format(storename,k,v,self.geoserver.get_datastore_field(storedata,k)))
                self._add_container_resource("workspaces",wsname,"datastores",storename,parameters=parameters)
                self._update_checklist("datastore",operation,[True,"Succeed to create the datastore '{}.{}' for postgis db".format(wsname,storename)])
                self._update_checklist("datastore",operation,self.post_create_postgisdatastore(wsname,storename,parameters))
                return storename
            else:
                self._update_checklist("datastore",operation,[False,"Failed to create the datastore '{}.{}' for postgis db".format(wsname,storename)])
                return None
        except Exception as ex:
            self._update_checklist("datastore",operation,[False,"Failed to create the datastore '{}.{}' for postgis db. {}".format(wsname,storename,ex)])
            return None

    def post_create_postgisdatastore(self,wsname,storename,parameters):
        return None

    def update_postgisdatastore(self):
        """
        create the test workspace if doesn't have
        Return the created datastore. Return None if postgis is not configured, or create failed
        """
        operation = "update postgis datastore"
        for wsname,wsdata in self._resources.get("workspaces",{}).items():
            if wsname == "__parameters__":
                continue
            for storename,storedata in wsdate.get("datastores",{}).items():
                if storename == "__parameters__":
                    continue
                if not storename.startwith("postgisds"):
                    continue
                try:
                    #change the parameters' value
                    original_parameters = storedata["__parameters__"]
                    parameters = {}
                    for key in ["Connection timeout","Max connection idle time","min connections","max connections","fetch size"]:
                        original_parameters[key] = original_parameters[key]  + 10
                        parameters[key] = original_parameters[key]

                    self.geoserver.update_datastore(wsname,storename,parameters,create=False)
                    storedata = self.geoserver.get_datastore(wsname,storename)
                    for k,v in parameters.items():
                        if k == "passwd":
                            continue
                        if self.geoserver.get_datastore_field(storedata,k) != str(v):
                            raise Exception("The field({1}) of the datastore({0}) should be {3} instead of {2}".format(storename,k,v,self.geoserver.get_datastore_field(storedata,k)))

                    self._update_checklist("datastore",operation,[True,"Succeed to update the datastore '{}.{}' for postgis db".format(wsname,storename)])
                    self._update_checklist("datastore",operation,self.post_update_postgisdatastore(wsname,storename,parameters))
                except Exception as ex:
                    self._update_checklist("datastore",operation,[False,"Failed to update the datastore '{}.{}' for postgis db. {}".format(wsname,storename,ex)])

    def post_update_postgisdatastore(self,wsname,storename,parameters):
        return None

    def delete_datastore(self):
        for wsname,wsdata in self._resources.get("workspaces",{}).items():
            if wsname == "__parameters__":
                continue
            for storename in wsdate.get("datastores",{}).keys():
                if storename == "__parameters__":
                    continue
                try:
                    self.geoserver.delete_datastore(wsname,storename)
                    if self.geoserver.has_datastore(wsname,storename):
                        self._update_checklist("datastore","delete",[False,"Failed to delete the datastore '{}.{}'".format(wsname,storename)])
                    else:
                        self._update_checklist("datastore","delete",[True,"Succeed to delete the datastore '{}.{}'".format(wsname,storename)])
                        self._update_checklist("datastore","delete",self.post_delete_datastore(wsname,storename))
                except Exception as ex:
                    self._update_checklist("datastore","delete",[False,"Failed to delete the datastore '{}.{}. {}'".format(wsname,storename,ex)])

    def post_delete_datastore(self,wsname,storename):
        return None

    def _create_styles(self,wsname,dataset,folder=None):
        #find the style folder and layer name
        stylefolder,layername = os.path.split(dataset)
        layername = os.path.splitext(layername)[0]
        if not stylefolder:
            stylefolder = self.style_folder
        if not stylefolder:
            #not style found
            return

        #find the style 
        styles = []
        for f in os.listdir(stylefolder):
            basename,fileext = os.path.splitext(f)
            if fileext != ".sld":
                continue
            prefix = "{}.".format(layername)
            if not basename.startswith(prefix):
                continue
            try:
                stylename,styleversion = layername[len(prefix):].split(".",1)
            except ValueError as ex:
                stylename = layername[len(prefix):]
                styleversion = "1.0.0"
            except:
                continue
            styles.appoend(["{}_{}".format(layername,stylename),styleversion,f])

        #sort the style, always put the style 'default' as the first item
        styles.sort(key=lambda s: "0{}".format(s[0])  if s[0] =="default" else s[0])

        for stylename,styleversion,f in styles:
            try:
                with open(os.path.join(stylefolder,f),'r') as fin:
                    styledata = fin.read()
                    self.geoserver.update_style(wsname,stylename,styleversion,styledata)
                if self.geoserver.has_style(wsname,stylename)
                    self._add_container_resource(None,"workspaces",wsname,"styles",layername,stylename,parameters={"version":styleversion,"style":styledata})
                    self._update_checklist("style","create",[True,"Succeed to create the style '{}.{}'".format(wsname,stylename)])
                    self._update_checklist("style","create",self.post_create_style(wsname,stylename,styleversion,styledata))
                else:
                    self._update_checklist("style","create",[False,"Failed to create the style '{}.{}'".format(wsname,stylename)])
            except Exception as ex:
                self._update_checklist("style","create",[False,"Failed to create the style '{}.{}'. {}".format(wsname,stylename,ex)])

    def post_create_style(self,wsname,stylename,styleversion,styledata):
        return None

    def update_style(self):
        for wsname,wsdata in self._resources.get("workspaces",{}).items():
            if wsname == "__parameters__":
                continue
            for storename,storedata in wsdate.get("datastores",{}).items():
                if storename == "__parameters__":
                    continue
                for layername,layerdata in storedata.items():
                    if layerename == "__parameters__":
                        continue
                    if not layerdata.get("defaultStyle") or not layerdata.get("styles"):
                        continue
                    defaultstyle = layerdata.get("defaultStyle")
                    style = layerdata.get("styles")[0]
                    featuretypedata = self.geoserver.get_featuretype(wsname,layername,storename=storename)
                    if not featuretypedata:
                        raise Exception("The featuretype({}.{}.{}) doesn't exist".format(wsname,storename,layername))
                    bbox = self.geoserver.get_featuretype_field(featuretypedata,"latLonBoundingBox")
                    if not bbox:
                        raise Exception("Can't find the bbox of the featuretype({}.{}.{}) doesn't exist".format(wsname,storename,layername))
                    srs = self.geoserver.get_featuretype_field(featuretypedata,"srs")
                    if not srs:
                        raise Exception("Can't find the srs of the featuretype({}.{}.{}) doesn't exist".format(wsname,storename,layername))
                    
                    image_defaultstyle = None
                    image_style = None
                    image_defaultstyle_updated = None
                    image_defaultstyle_restored = None
                    try:
                        image_defaultstyle = self.geoserver.get_map(self,workspace,layername,bbox,srs=srs,width=1024,height=1024,format="image/jpeg")
                        image_style = self.geoserver.get_map(self,workspace,layername,bbox,srs=srs,width=1024,height=1024,format="image/jpeg",style=style)
                        if filecmp.cmp(image_defaultstyle,image_style):
                            raise Exception("The image of layer({}.{}.{}) from different style should be different.".format(wsname,storename,layername))
                        #update style
                        self.geoserver.update_style(wsname,defaultstyle,*self._resources[wsname]["styles"][layername][style]["__parameters__"])
                        image_defaultstyle_updated = self.geoserver.get_map(self,workspace,layername,bbox,srs=srs,width=1024,height=1024,format="image/jpeg")
                        if not filecmp.cmp(image_defaultstyle_updated,image_style):
                            raise Exception("The image of layer({}.{}.{}) with same style  should be same. style={}".format(wsname,storename,layername,style))
                        else:
                            self._update_checklist("style","update",[True,"Succeed to update the style '{}.{}.{}'".format(wsname,layername,stylename)])
                            self.post_update_style(wsname,layername,defaultstyle)

                        #restore the updated style
                        self.geoserver.update_style(wsname,defaultstyle,sekf._resources[wsname]["styles"][layername][defaultstyle]["__parameters__"])
                        image_defaultstyle_restored = self.geoserver.get_map(self,workspace,layername,bbox,srs=srs,width=1024,height=1024,format="image/jpeg")
                        if not filecmp.cmp(image_defaultstyle_resoted,image_defaultstyle):
                            raise Exception("The image of layer({}.{}.{}) with same style  should be same.style={}".format(wsname,storename,layername,defaultstyle))
                        else:
                            self._update_checklist("style","update",[True,"Succeed to restore the style '{}.{}.{}'".format(wsname,layername,defaultstyle)])
                            self.post_update_style(wsname,layername,defaultstyle)
                    except Exception as ex:
                        self._update_checklist("style","update",[False,"Failed to update the style '{}.{}.{}'. {}".format(wsname,layername,defaultstyle,ex)])
                    finally:
                        if image_defaultstyle:
                            utils.remove_file(image_defaultstyle)
                        if image_style:
                            utils.remove_file(image_style)
                        if image_defaultstyle_updated:
                            utils.remove_file(image_defaultstyle_updated)
                        if image_defaultstyle_restored:
                            utils.remove_file(image_defaultstyle_restored)

    def post_update_style(self,wsname,layername,stylename):
        return None

    def delete_style(self):
        for wsname,wsdata in self._resources.get("workspaces",{}).items():
            if wsname == "__parameters__":
                continue
            for layername,styles in wsdate.get("styles",{}).items():
                if layername == "__parameters__":
                    continue
                for stylename in styles.keys():
                    if stylename == "__parameters__":
                        continue
                    try:
                        self.geoserver.delete_style(wsname,stylename)
                        if self.geoserver.has_style(wsname,stylename):
                            self._update_checklist("style","delete",[False,"Failed to delete style '{}.{}.{}'".format(wsname,layername,stylename)])
                        else:
                            self._update_checklist("style","delete",[True,"Succeed to delete style '{}.{}.{}'".format(wsname,layername,stylename)])
                            self._update_checklist("style","delete",self.post_delete_style(wsname,layername,stylename))
                    except Exception as ex:
                        self._update_checklist("style","delete",[False,"Failed to delete style '{}.{}.{}'. {}".format(wsname,layername,stylename,ex)])

    def post_delete_style(self,wsname,layername,stylename):
        return None

    def publish_featuretype_from_localdatastore(self,wsname,storename,dataset):
        """
        Publish featuretype from local dataset
        Return layername if succeed; otherwise return None
        """
        layername = os.path.splitext(os.path.basename(dataset))[0]
        operation = "Publish featuretype from local datastore"
        parameters = {"nativeName":layername,"title":layername,"abstract":"for testing","keywords":["test"]}
        try:
            self.geoserver.publish_featuretype(wsname,storename,layername,parameters,create=True)
            if has_featuretype(wsname,layername,storename):
                featuretypedata = self.geoserver.get_featuretype(wsname,layername,storename)
                for k in ("title","abstract","keywords"):
                    v = parameters[k]
                    self.assertEqual(self.geoserver.get_featuretype_field(featuretypedata,k),v if isinstance(v,list) else str(v),"The field({1}) of the datastore({0}) should be {3} instead of {2}".format(layername,k,v,self.geoserver.get_datastore_field(featuretypedata,k)))

                self._add_container_resource("workspaces",wsname,"datastores",storename,layername)
                self._update_checklist("featuretype",operation,[True,"Succeed to create the featuretype '{}.{}.{}' from local dataset".format(wsname,storename,layername)])
                self._update_checklist("featuretype",operation,self.post_publish_featuretype_from_localdatastore(wsname,storename,layername,parameers))
                return layernname
            else:
                self._update_checklist("featuretype",operation,[False,"Failed to create the featuretype '{}.{}.{}' from local dataset".format(wsname,storename,layername)])
                return None
        except Exception as ex:
            self._update_checklist("featuretype",operation,[False,"Failed to create the featuretype '{}.{}.{}' from local dataset. {}".format(wsname,storename,layername,ex)])
            return None

    def post_publish_feturetype_from_localdatastore(self,wsname,storename,layername,parameters):
        return None

    def publish_featuretype_from_postgis(self,wsname,storename):
        """
        Publish the feature type from postgis
        return the published layes if published; otherwise return empty list

        """
        operation = "Publish featuretype from postgis datastore"
        parameters = {"nativeName":layername}
        layers = []
        if os.environ.get("POSTGIS_TABLE"):
            layers.append((
                os.environ["POSTGIS_TABLE"],
                {
                    "srs":"EPSG:4326",
                    "nativeName":os.environ["POSTGIS_TABLE"]
                }
            ))
        if all(os.environ.get(key) is not None  for key in ("POSTGIS_GEOMETRY_COLUMN","POSTGIS_GEOMETRY_TYPE","POSTGIS_TABLE"))
            layers.append((
                "{}_view".format(os.environ["POSTGIS_TABLE"]),
                {
                    "srs":"EPSG:4326",
                    "viewsql":"select * from {}".format(os.environ["POSTGIS_TABLE"]),
                    "geometry_column":os.environ["POSTGIS_GEOMETRY_COLUMN"],
                    "geometry_type":os.environ["POSTGIS_GEOMETRY_TYPE"]
                }
            ))
        published_layers = []
        for layername,parameters in layers:
            try:
                self.geoserver.publish_featuretype(wsname,storename,layername,parameters,create=True)
                if self.geoserver.has_featuretype(wsname,layername,storename=storename):
                    featuretypedata = self.geoserver.get_featuretype(wsname,layername,storename)
                    for k in ("title","abstract","keywords"):
                        v = parameters[k]
                        self.assertEqual(self.geoserver.get_featuretype_field(featuretypedata,k),v if isinstance(v,list) else str(v),"The field({1}) of the datastore({0}) should be {3} instead of {2}".format(layername,k,v,self.geoserver.get_datastore_field(featuretypedata,k)))

                    self._add_container_resource("workspaces",wsname,"datastores",storename,layername,parameters=parameters)
                    self._update_checklist("featuretype",operation,[True,"Succeed to publish the featuretype '{}.{}.{}' from postgis table".format(wsname,storename,layername)])
                    published_layers.append(layername)
                    self._update_checklist("featuretype",operation,self.post_publish_featuretype_from_postgisdatastore(wsname,storename,layername,parameters))
                else:
                    self._update_checklist("featuretype",operation,[False,"Failed to publish the featuretype '{}.{}.{}' from postgis table".format(wsname,storename,layername)])
            except Exception as ex:
                self._update_checklist("featuretype",operation,[False,"Failed to publish the featuretype '{}.{}.{}' from postgis table. {}".format(wsname,storename,layername,ex)])

        return published_layers

    def post_publish_feturetype_from_postgisdatastore(self,wsname,storename,layername,parameters):
        return None

    def update_featuretype(self):
        operation = "update"
        for wsname,wsdata in self._resources.get("workspaces",{}).items():
            if wsname == "__parameters__":
                continue
            for storename,storedata in wsdate.get("datastores",{}).items():
                if storename == "__parameters__":
                    continue
                for layername,layerdata in storedata.items():
                    if layername == "__parameters__":
                        continue
                    original_parameters = layerdata["__parameters__"]
                    parameters = {}
                    for k in ("title","abstract","keywords"):
                        if k == "keywords":
                            original_parameters[k].append("testkeyword_{}".format(len(original_parameters[k])))
                        else:
                            original_parameters[k] = "{} changed".format(original_parameters[k])
                        parameters[k] = original_parameters[k]

                    try:
                        self.geoserver.publish_featuretype(wsname,storename,layername,parameters,create=False)
                        featuretypedata = self.geoserver.get_featuretype(wsname,layername,storename)
                        for k in ("title","abstract","keywords"):
                            v = parameters[k]
                            self.assertEqual(self.geoserver.get_featuretype_field(featuretypedata,k),v if isinstance(v,list) else str(v),"The field({1}) of the datastore({0}) should be {3} instead of {2}".format(layername,k,v,self.geoserver.get_datastore_field(featuretypedata,k)))

                            self._update_checklist("featuretype",operation,[True,"Succeed to update featuretype '{}.{}.{}'".format(wsname,storename,layername)])
                            self._update_checklist("featuretype",operation,self.post_update_featuretype(wsname,storename,layername))
                    except Exception as ex:
                        self._update_checklist("featuretype",operation,[False,"Failed to delete featuretype '{}.{}.{}'. {}".format(wsname,storename,layername,ex)])

    def post_update_featuretype(self,wsname,storename,layername):
        return None

    def update_featuretypestyles(self):
        operation = "update featuretype style"
        for wsname,wsdata in self._resources.get("workspaces",{}).items():
            if wsname == "__parameters__":
                continue
            for layername,layerdata in wsdate.get("styles",{}).items():
                if layername == "__parameters__":
                    continue
                #find the defaultstyle and styles
                defaultstyle = layerdata.get("default")
                styles = [style for style in layerdata.keys() if style != "default"]
                styles.sort()
                if not defaultstyle and styles:
                    defaultstyle = styles[0]
                    del styles[0]

                if not defaultstyle and not styles:
                    continue
                
                self.geoserver.set_featuretype_styles(wsname,layername,defaultstyle,styles)
                layerstyles = self.geoserver.get_featuretype_styles(wsname,layername)
                if defaultstyle != layerstyles[0] or set(styles) != set(layerstyles[1]):
                    self._update_checklist("featuretype",operation,[False,"Failed to set the default style({2}) or the styles({3}) of the featuretype '{0}.{1}'".format(wsname,layername,defaultstyle,styles)])
                else:
                    self._update_checklist("featuretype",operation,[True,"Succeed to set the default style({2}) or the styles({3}) of the featuretype '{0}.{1}'".format(wsname,layername,defaultstyle,styles)])
                    self._update_checklist("featuretype",operation,self.post_update_featuretype_styles(wsname,layername,defaultstyle,styles))
                                                                    

    def post_update_featuretype_styles(wsname,layername,defaultstyle,styles):
        return None

    def delete_featuretype(self):
        operation = "delete"
        for wsname,wsdata in self._resources.get("workspaces",{}).items():
            if wsname == "__parameters__":
                continue
            for storename,storedata in wsdate.get("datastores",{}).items():
                if storename == "__parameters__":
                    continue
                for layername in storedata.keys():
                    if layername == "__parameters__":
                        continue
                    try:
                        self.geoserver.delete_featuretype(wsname,storename,layername)
                        if self.geoserver.has_featuretype(wsname,storename,layername):
                            self._update_checklist("featuretype",operation,[False,"Failed to delete featuretype from local dataset '{}.{}.{}'".format(wsname,storename,layername)])
                        else:
                            self._update_checklist("featuretype",operation,[True,"Succeed to delete featuretype from local dataset '{}.{}.{}'".format(wsname,storename,layername)])
                            self._update_checklist("featuretype",operation,self.post_delete_featuretype(wsname,storename,layername))
                    except Exception as ex:
                        self._update_checklist("featuretype",operation,[False,"Failed to delete featuretype from local dataset '{}.{}.{}'. {}".format(wsname,storename,layername,ex)])

    def post_delete_feturetype(self,wsname,storename,layername):
        return None


    def create_wmsstore(self,wsname):
        """
        Use the same geoserver as the upstream server
        """
        try:
            storename = "wmsstore4compatibilitycheck"
            parameters = {
                "capabilitiesURL": "{}/ows?service=WMS&version=1.1.1&request=GetCapabilities".format(os.environ.get("GEOSERVER_URL")),
                "user": os.environ.get("GEOSERVER_USER",None),
                "password": os.environ.get("GEOSERVER_PASSWORD",None),
                "maxConnections": 10,
                "readTimeout": 300,
                "connectTimeout": 60
            }
            self.geoserver.update_wmsstore(wsname,storename,parameters,create=True)
            if self.geoserver.has_wmsstore(wsname,storename):
                storedata = self.geoserver.get_wmsstore(wsname,storename)
                for key in ("maxConnections","readTimeout","connectTimeout"):
                    if self.geoserver.get_wmsstore_field(storedata,key) != str(parameters[key]):
                        raise Exception("The the parameter({2}) of the wmsstore({0}.{1}) should be instead of {3}".format(wsname,storename,parameters[key],self.geoserver.get_wmsstore_field(storedata,key)))
                self._add_container_resource("workspaces",wsname,"wmsstores",storename,parameters=parameters)
                self._update_checklist("wmsstore","create"],[True,"Succeed to create the wmsstore '{}.{}'".format(wsname,storename)])
                self._update_checklist("wmsstore","create"],self.post_create_wmsstore(wsname,storename,parameters))
                return storename
            else:
                self._update_checklist("wmsstore","create"],[False,"Failed to create the wmsstore '{}.{}'".format(wsname,storename)])
                return None
        except Exception as ex:
            self._update_checklist("wmsstore","create"],[False,"Failed to create the wmsstore '{}.{}'. {}".format(wsname,storename,ex)])
            return None

    def post_create_wmsstore(self,wsname,storename,parameters):
        pass

    def update_wmstore(self):
        operation = "update"
        for wsname,wsdata in self._resources.get("workspaces",{}).items():
            if wsname == "__parameters__":
                continue
            for storename,storedata in wsdate.get("wmsstores",{}).items():
                if storename == "__parameters__":
                    continue
                original_parameters = storedata["__parameters__"]
                parameters = {}
                for key in ("maxConnections","readTimeout","connectTimeout"):
                    original_parameters[key] = original_parameters[key] + 10
                    parameters[key] = original_parameters[key]

                try:
                    self.geoserver.update_wmsstore(wsname,storename,parameters,create=True)
                    wmsstoredata = self.geoserver.get_wmsstore(wsname,storename)
                    for key in ("maxConnections","readTimeout","connectTimeout"):
                        if self.geoserver.get_wmsstore_field(wmsstoredata,key) != str(parameters[key]):
                            raise Exception("The the parameter({2}) of the wmsstore({0}.{1}) should be instead of {3}".format(wsname,storename,parameters[key],self.geoserver.get_wmsstore_field(wmsstoredata,key)))
                    self._update_checklist("wmsstore",operation,[True,"Succeed to update wms store '{}.{}'".format(wsname,storename)])
                    self._update_checklist("wmsstore",operation,self.post_update_wmsstore(wsname,storename,parameters))
                except Exception as ex:
                    self._update_checklist("wmsstore",operation,[False,"Failed to update wms store '{}.{}'. {}".format(wsname,storename,ex)])

    def post_update_wmsstore(wsname,storename,parameters):
        return None

    def delete_wmsstore(self):
        for wsname,wsdata in self._resources.get("workspaces",{}).items():
            if wsname == "__parameters__":
                continue
            for storename in wsdate.get("wmsstores",{}).keys():
                if storename == "__parameters__":
                    continue
                try:
                    self.geoserver.delete_wmsstore(wsname,storename)
                    if self.geoserver.has_wmsstore(wsname,storename):
                        self._update_checklist("wmsstore","delete",[False,"Failed to delete wms store '{}.{}'".format(wsname,storename)])
                    else:
                        self._update_checklist("wmsstore","delete",[True,"Succeed to delete wms store '{}.{}'".format(wsname,storename)])
                        self.post_delete_wmsstore(wsname,storename)
                except Exception as ex:
                    self._update_checklist("wmsstore","delete",[False,"Failed to delete wms store '{}.{}'. {}".format(wsname,storename,ex)])

    def post_delete_wmsstore(self,wsname,storename):
        pass

    def publish_wmslayer(self,wsname,storename):
        """
        Publish a wmslayer from upstream layer
        wmslayer should be published to a separate workspace
        Return the published layers if succeed; otherwise return None
        """
        for workspace,workspacedata in self._resources.get("workspaces",{}).items():
            if wsname == "__parameters__":
                continue
            if workspace == wsname:
                continue
            for dsname, dsdata in workspacedata.get("datastores",[]).items():
                if dsname == "__parameters__":
                    continue
                for layername in dsdata.keys():
                    if layername == "__parameters__":
                        continue
                    try:
                        parameters = {
                            "nativeName": "{}:{}".format(workspace,layername)
                        }
                        self.geoserver.update_wmslayer(wsname,storename,layername,parameters,True)
                        if self.geoserver.has_wmslayer(wsname,layername,storename=storename):
                            self._add_container_resource("workspaces",wsname,"wmsstores",storename,layername,parameters=parameters)
                            self._update_checklist("wmslayer","create"],[True,"Succeed to publish the wms layer '{}.{}.{}'".format(wsname,storename,layername)])
                            self.post_publish_wmslayer(wsname,storename,layername,parameters)
                            return layername
                        else:
                            self._update_checklist("wmslayer","create"],[False,"Failed to publish the wms layer '{}.{}.{}'".format(wsname,storename,layername)])
                            continue
                    except Exception as ex:
                        self._update_checklist("wmslayer","create"],[False,"Failed to publish the wms layer '{}.{}.{}'. {}".format(wsname,storename,layername,ex)])
                        continue

        return None

    def post_publish_wmslayer(self,wsname,storename,layername,parameters):
        pass
    
    def update_wmslayers(self):
        operation = "update"
        for wsname,wsdata in self._resources.get("workspaces",{}).items():
            if wsname == "__parameters__":
                continue
            for storename,storedata in wsdata.get("wmsstores",{}).items():
                if storename == "__parameters__":
                    continue
                for layername,layerdata in storename.items():
                    if layername == "__parameters__":
                        continue
                    original_parameters = layerdata["__parameters__"]
                    parameters = {}
                    for key in ("tile","abstract","description"):
                        original_parameters[key] = "{} changed".format(original_parameters[key])
                        parameters[key] = original_parameters[key]

                    try:
                        self.geoserver.update_wmslayer(wsname,storename,layername,parameters,create=False)
                        wmslayerdata = self.geoserver.get_wmsstore(wsname,storename)
                        for key in ("title","abstract","description"):
                            if self.geoserver.get_wmslayer_field(wmslayerdata,key) != parameters[key]:
                                raise Exception("The the parameter({3}) of the wmslayer({0}.{1}.{2}) should be instead {4} of {5}".format(wsname,storename,layername,key,parameters[key],self.geoserver.get_wmslayer_field(wmslayerdata,key)))

                        self._update_checklist("wmslayer",operation],[True,"Succeed to update the wms layer '{}.{}.{}'. {}".format(wsname,storename,layername,ex)])
                        self._update_checklist("wmslayer",operation],self.post_update_wmslayer(wsname,storename,layername,parameters))
                    except Exception as ex:
                        self._update_checklist("wmslayer",operation],[False,"Failed to update the wms layer '{}.{}.{}'. {}".format(wsname,storename,layername,ex)])

    def post_update_wmslayer(self,workspace,storename,layername,parameters):
        return None

    def delete_wmslayers(self):
        operation = "delete"
        for wsname,wsdata in self._resources.get("workspaces",{}).items():
            if wsname == "__parameters__":
                continue
            for storename,storedata in wsdata.get("wmsstores",{}).items():
                if storename == "__parameters__":
                    continue
                for layername,layerdata in storename.items():
                    if layername == "__parameters__":
                        continue
                    try:
                        self.geoserver.delete_wmslayer(wsname,layername)
                        if self.geoserver.has_wmslayer(wsname,layername):
                            self._update_checklist("wmslayer",operation],[False,"Failed to delete the wms layer '{}.{}.{}'.".format(wsname,storename,layername)])
                        else:
                            self._update_checklist("wmslayer",operation],[True,"Succeed to delete the wms layer '{}.{}.{}'.".format(wsname,storename,layername)])
                            self._update_checklist("wmslayer",operation],self.post_delete_wmslayer(wsname,storename,layername))
                    except Exception as ex:
                        self._update_checklist("wmslayer",operation],[False,"Failed to delete the wms layer '{}.{}.{}'. {}".format(wsname,storename,layername,ex)])

    def post_delete_wmslayer(self,wsname,storename,layername):
        return None

    def create_layergroup(self,wsname):
        """
        Create a layergroup 
        Return the layergroup if succeed;otherwise return None
        """
        try:
            parameters={"layers":[],"title":"for testing"}
            groupname="layergroup4compatibilitycheck"
            for workspace,workspacedata in self._resource.get("workspaces").item():
                for dsname, dsdata in workspacedata.get("datastores",[]).items():
                    for layername in dsdata.keys():
                        try:
                            parameters["layers"].append({"type":"layer","name":layername,"workspace":workspace})

            if parameters["layers"]:
                self.geoserver.update_layergroup(wsname,groupname,parameters,create=true)
                if self.geoserver.has_wmslayer(wsname,layername,storename=storename):
                    self._set_resource(parameters,"workspaces",wsname,"layergroups",groupname)
                    self._update_checklist("layergroup","create"],[True,"Succeed to create layergroup '{}.{}'".format(wsname,layername)])
                    self.post_create_layergroup(wsname,groupname,parameters)
                    return groupname
                else:
                    self._update_checklist("layergroup","create"],[False,"Failed to create layergroup '{}.{}'".format(wsname,layername)])
                    return None
            else:
                return None
        except Exception as ex
            self._update_checklist("layergroup","create"],[False,"Failed to create layergroup '{}.{}'. {}".format(wsname,layername,ex)])
            return None

    def post_create_layergroup(self,workspace,groupname,parameters):
        pass


    def update_layergroup(self):
        for wsname,wsdata in self._resources.get("workspaces",{}).items():
            if wsname == "__parameters__":
                continue
            for groupname,parameters in wsdate.get("layergroups",{}).items():
                if groupname == "__parameters__":
                    continue
                try:
                    if len(parameters["layers"]) < 2:
                         continue
                    parameters["layers"].sort(reverse=True)
                    parameters["title"] = "{} changed".format(parameters.get("title","for testing"))
                    self.geoserver.update_layergroup(wsname,groupname,parameters,create=False)
                    layergroupdata = self.geoserver.get_layergroup(wsname,groupname)
                    for key in ("title","layers"):
                        if self.geoserver.get_layergroup_field(layergroupdata,key) != parameters[key]:
                            raise Exception("The the parameter({2}) of the layergroup({0}.{1}) should be {3} instead of {4}".format(wsname,groupname,key,parameters[key],self.geoserver.get_layergroup_field(layergroupdata,key)))
                    
                    self._update_checklist("layergroup","update",[True,"Succeed to update layergroup '{}.{}'".format(wsname,groupname)])
                    self._update_checklist("layergroup","update",self.post_update_layergroup(wsname,groupname,parameters))
                except Exception as ex:
                    self._update_checklist("layergroup","update",[False,"Failed to update layergroup '{}.{}'. {}".format(wsname,groupname,ex)])

    def post_update_layergroup(workspace,groupname,parameters):
        return None

    def delete_layergroup(self):
        for wsname,wsdata in self._resources.get("workspaces",{}).items():
            if wsname == "__parameters__":
                continue
            for groupname in wsdate.get("layergroups",{}).keys():
                if groupname == "__parameters__":
                    continue
                try:
                    self.geoserver.delete_layergroup(wsname,groupname)
                    if self.geoserver.has_layergroup(wsname,groupname):
                        self._update_checklist("layergroup","delete",[False,"Failed to delete the layergroup '{}.{}'".format(wsname,groupname)])
                    else:
                        self._update_checklist("layergroup","delete",[True,"Succeed to delete the layergroup '{}.{}'".format(wsname,groupname)])
                        self.post_delete_layergroup(wsname,groupname)
                except Exception as ex:
                    self._update_checklist("layergroup","delete",[False,"Failed to delete the layergroup '{}.{}'. {}".format(wsname,groupname,ex)])

    def post_delete_layergroup(self,workspace,groupname):
        return None


    def create_wmtslayer(self):
        gridSubsets = os.environ.get("GRIDSUBSETS")
        if gridSubsets:
            gridSubsets = [s.strip() for s in gridSubsets.split(",") if s.strip()]
        else:
            gridSubsets = ["gda94","mercator"]


        for wsname,wsdata in self._resources.get("workspaces",{}).items():
            #create wmts layer for feature type and wmslayer
            operation = "create gwc layer for featuretype"
            for storename,storedata in wsdata.get("datastores",{}).items():
                if storename == "__parameters__":
                    continue
                for layername,layerdata in storedata.items():
                    if layername == "__parameters__":
                        continue
                    parameters = {
                        "enabled":True,
                        "expireCache" : 3600,
                        "expireClients": 3600,
                        "gutter": 100
                        "gridSubsets" : []
                    }
                    for s in gridSubsets:
                        parameters["gridSubsets"].append({"name":s})
                    try:
                        self.geoserver.update_gwclayer(wsname,layername,parameters)
                        if self.geoserver.has_gwclayer(wsname,layername):
                            layerdata = self.geoserver.get_gwclayer(wsname,layername)
                            for field in ("enabled","expireCache","expireClients","gutter"):
                                if self.get_gwclayer_field(layerdata,field) != parameters[field]
                                    raise Exception("The field({}) should be {} instead of {}".format(field,parameters[field],self.get_gwclayer_field(layerdata,field)))

                            self._set_resource(parameters,"workspaces",wsname,"gwclayers",layername)
                            self._update_checklist("gwclayer",operation,[True,"Succeed to create the gwc layer '{}.{}'.".format(wsname,layername)])
                            self._update_checklist("gwclayer",operation,self.post_create_gwclayer(wsname,layername,parameters))
                        else:
                            self._update_checklist("gwclayer",operation,[False,"Failed to create the gwc layer '{}.{}'.".format(wsname,layername)])
                    except Exception as ex
                        self._update_checklist("gwclayer",operation,[False,"Failed to create the gwc layer '{}.{}'. {}".format(wsname,layername,ex)])

            operation = "create gwc layer for wms layer"
            for storename,storedata in wsdata.get("wmsstores",{}).items():
                if storename == "__parameters__":
                    continue
                for layername,layerdata in storedata.items():
                    if layername == "__parameters__":
                        continue
                    parameters = {
                        "enabled":True,
                        "expireCache" : 3600,
                        "expireClients": 3600,
                        "gutter": 100
                        "gridSubsets" : []
                    }
                    for s in gridSubsets:
                        parameters["gridSubsets"].append({"name":s})
                    try:
                        self.geoserver.update_gwclayer(wsname,layername,parameters)
                        if self.geoserver.has_gwclayer(wsname,layername):
                            layerdata = self.geoserver.get_gwclayer(wsname,layername)
                            for field in ("enabled","expireCache","expireClients","gutter"):
                                if self.get_gwclayer_field(layerdata,field) != parameters[field]
                                    raise Exception("The field({}) should be {} instead of {}".format(field,parameters[field],self.get_gwclayer_field(layerdata,field)))

                            self._set_resource(parameters,"workspaces",wsname,"gwclayers",layername)
                            self._update_checklist("gwclayer",operation,[True,"Succeed to create the gwc layer '{}.{}'.".format(wsname,layername)])
                            self._update_checklist("gwclayer",operation,self.post_create_gwclayer(wsname,layername,parameters))
                        else:
                            self._update_checklist("gwclayer",operation,[False,"Failed to create the gwc layer '{}.{}'.".format(wsname,layername)])
                    except Exception as ex
                        self._update_checklist("gwclayer",operation,[False,"Failed to create the gwc layer '{}.{}'. {}".format(wsname,layername,ex)])

            #create wmts layer for layergroup
            operation = "create gwc layer for layergroup"
            for layername in wsdata.get("layergroups",{}).keys():
                if layername == "__parameters__":
                    continue
                parameters = {
                    "enabled":True,
                    "expireCache" : 3600,
                    "expireClients": 3600,
                    "gutter": 100
                    "gridSubsets" : []
                }
                for s in gridSubsets:
                    parameters["gridSubsets"].append({"name":s})
                try:
                    self.geoserver.update_gwclayer(wsname,layername,parameters)
                    if self.geoserver.has_gwclayer(wsname,layername):
                        layerdata = self.geoserver.get_gwclayer(wsname,layername)
                        for field in ("enabled","expireCache","expireClients","gutter"):
                            if self.get_gwclayer_field(layerdata,field) != parameters[field]
                                raise Exception("The field({}) should be {} instead of {}".format(field,parameters[field],self.get_gwclayer_field(layerdata,field)))

                        self._set_resource(parameters,"workspaces",wsname,"gwclayers",layername)
                        self._update_checklist("gwclayer",operation,[True,"Succeed to create the gwc layer '{}.{}'.".format(wsname,layername)])
                        self._update_checklist("gwclayer",operation,self.post_create_gwclayer(wsname,layername,parameters))
                    else:
                        self._update_checklist("gwclayer",operation,[False,"Failed to create the gwc layer '{}.{}'.".format(wsname,layername)])
                except Exception as ex
                    self._update_checklist("gwclayer",operation,[False,"Failed to create the gwc layer '{}.{}'. {}".format(wsname,layername,ex)])

    def post_create_gwclayer(self,workspace,layername,parameters):
        return None

    def enable_wmtslayers(self,enable):
        for wsname,wsdata in self._resources.get("workspaces",{}).items():
            for layername in wsdate.get("gwclayers",{}).keys():
                if layername == "__parameters__":
                    continue
                try:
                    self.geoserver.delete_gwclayer(wsname,layername)
                    if self.geoserver.has_gwclayer(wsname,layername):
                        self._update_checklist("gwclayer","delete",[False,"Failed to delete the gwc layer '{}.{}'.".format(wsname,layername)])
                    else:
                        self._update_checklist("gwclayer","delete",[True,"Succeed to delete the gwc layer '{}.{}'.".format(wsname,layername)])
                        self._update_checklist("gwclayer","delete",self.post_delete_gwclayer(wsname,layernamename))
                except Exception as ex:
                    self._update_checklist("gwclayer","delete",[False,"Failed to delete the gwc layer '{}.{}'. {}".format(wsname,layername,ex)])


    def post_enable_wmtslayer(self,workspace,layername,enable):
        return None

    def check_wmts_expire(self):
        pass

    def check_wmts_clear_cache(self):
        pass

    def delete_wmtslayer(self):
        for wsname,wsdata in self._resources.get("workspaces",{}).items():
            for layername in wsdate.get("gwclayers",{}).keys():
                if layername == "__parameters__":
                    continue
                try:
                    self.geoserver.delete_gwclayer(wsname,layername)
                    if self.geoserver.has_gwclayer(wsname,layername):
                        self._update_checklist("gwclayer","delete",[False,"Failed to delete the gwc layer '{}.{}'.".format(wsname,layername)])
                    else:
                        self._update_checklist("gwclayer","delete",[True,"Succeed to delete the gwc layer '{}.{}'.".format(wsname,layername)])
                        self._update_checklist("gwclayer","delete",self.post_delete_gwclayer(wsname,layernamename))
                except Exception as ex:
                    self._update_checklist("gwclayer","delete",[False,"Failed to delete the gwc layer '{}.{}'. {}".format(wsname,layername,ex)])

    def post_delete_gwclayer(self,workspace,layername):
        return None

    def create_usergroup(self):
        usergroup = "usergroup4compatibilitycheck"
        try:
            self.geoserver.add_usergroup(usergroup)
            if self.has_usergroup(usergroup):
                self._set_resource({},"usergroups",usergroup)
                self._update_checklist("usergroup","create",[True,"Succeed to add the usergroup '{}'.".format(usergroup)])
                self._update_checklist("usergroup","create",self.post_create_usergroup(groupname))
            else:
                self._update_checklist("usergroup","create",[False,"Failed to add the usergroup '{}'.".format(usergroup)])
        except Exception as ex:
            self._update_checklist("usergroup","create",[False,"Failed to add the usergroup '{}'. {}".format(usergroup,ex)])

    def post_create_usergroup(self,groupname):
        return None

    def delete_usergroup(self):
        for groupname in self._resources.get("usergroups",{}).keys():
            try:
                self.geoserver.delete_usergroup(groupname)
                if self.geoserver.has_usergroup(groupname):
                    self._update_checklist("usergroup","delete",[False,"Failed to delete the usergroup '{}'".format(groupname)])
                else:
                    self._update_checklist("usergroup","delete",[True,"Succeed to delete the usergroup '{}'".format(groupname)])
                    self._update_checklist("usergroup","delete",self.post_delete_usergroup(groupname))
            except Exception as ex:
                self._update_checklist("usergroup","delete",[False,"Failed to delete the usergroup '{}'. {}".format(groupname,ex)])

    def post_delete_usergroup(self,groupname):
        return  None

    def create_user(self):
        users=["user4compatibilitycheck{}@test.com".format(i) for i in range(1,3,1)]
        password = "1234""
        for user in users:
            try:
                self.geoserver.create_user(user,password,enable=True)
                if self.geoserver.has_user(user):
                    if self.geoserver.login(user,password):
                        self._set_resource({"password":password,"enabled":True},"users",user)
                        self._update_checklist("user","create",[True,"Succeed to add the user '{}'.".format(user)])
                        self._update_checklist("user","create",self.post_create_user(user,password,True))
                    else:
                        raise Exception("Can't login with the new user")
                else:
                    self._update_checklist("user","create",[False,"Failed to add the user '{}'.".format(user)])
            except Exception as ex:
                self._update_checklist("user","create",[False,"Failed to add the user '{}'. {}".format(user,ex)])

    def post_create_user(self,user,password,enable):
        return None

    def enable_user(self):
        for user,userdata in self._resources.get("users",{}).items():
            try
                self.geoserver.enable_user(user,False)
                if self.geoserver.get_user(user)[1]:
                    self._update_checklist("user","disable",[False,"Failed to disable the user '{}'".format(user)])
                else:
                    userdata["enabled"] = False
                    self._update_checklist("user","disable",[True,"Succeed to disable the user '{}'".format(user)])
                    self._update_checklist("user","disable",self.post_enable_user(user,False))
            except Exception as ex:
                self._update_checklist("user","disable",[False,"Failed to disable the user '{}'. {}".format(user,ex)])
                continue

            try
                self.geoserver.enable_user(user,True,create=False)
                if self.geoserver.get_user(user)[1]:
                    self._update_checklist("user","enable",[False,"Failed to enable the user '{}'".format(user)])
                else:
                    userdata["enabled"] = True
                    self._update_checklist("user","enable",[True,"Succeed to enable the user '{}'".format(user)])
                    self._update_checklist("user","disable",self.post_enable_user(user,True))
            except Exception as ex:
                self._update_checklist("user","enable",[False,"Failed to enable the user '{}'. {}".format(user,ex)])

    def post_enable_user(self,user,enable):
        return None

    def update_userpassword(self):
        password = "12345678"
        operation = "change password"
        for user,userdata in self._resources.get("users",{}).items():
            try
                self.geoserver.change_userpassword(user,password)
                if self.geoserver.login(user,password):
                    self._update_checklist("user",operation,[False,"Failed to change the password of the user '{}'".format(user)])
                else:
                    userdata["password"] = password
                    self._update_checklist("user",operation,[True,"Succeed to change the password of the user '{}'".format(user)])
                    self._update_checklist("user",operation,self.post_change_userpassword(user,password))
            except Exception as ex:
                self._update_checklist("user",operation,[False,"Failed to change the password of the user '{}'. {}".format(user,ex)])

    def post_change_userpassword(self,user,password):
        return None

    def delete_uesr(self):
        for user in self._resources.get("users",{}).keys():
            try
                self.geoserver.delete_user(user)
                if self.has_user(user):
                    self._update_checklist("user","delete",[False,"Failed to delete the user '{}'".format(user)])
                else:
                    self._update_checklist("user","delete",[True,"Succeed to delete the user '{}'".format(user)])
            except Exception as ex:
                self._update_checklist("user","delete",[False,"Failed to delete the user '{}'. {}".format(user,ex)])

    def add_user_to_group(self):
        operation = "add to group"
        for user,userdata in self._resources.get("users",{}).items():
            for group,groupdata in self._resources.get("usergroups",{}).items():
                try
                    self.geoserver.add_user_to_group(user,group)
                    if self.user_in_group(user,group):
                        if "groups" in userdata:
                            userdata["groups"].append(group)
                        else:
                            userdata["groups"] = [group]
                        if "users" in groupdata:
                            groupdata["users"].append(user)
                        else:
                            groupdata["users"] = [user]
                        self._update_checklist("user",operation,[True,"Succeed to add the user '{}' to usergroup '{}'".format(user,group)])
                        self._update_checklist("user",operation,self.post_add_user_to_group(user,group))
                    else:
                        self._update_checklist("user",operation,[False,"Failed to add the user '{}' to usergroup '{}'".format(user,group)])
                except Exception as ex:
                    self._update_checklist("user",operation,[False,"Failed to add the user '{}' to usergroup '{}'. {}".format(user,group,ex)])

    def post_add_user_to_group(self,user,group):
        return None

    def delete_user_from_group(self):
        operation = "delete from group"
        for user,userdata in self._resources.get("users",{}).items():
            if not userdata.get("groups"):
                continue
            for i in range(len(userdata["groups"]) - 1,-1,-1 ):
                group = userdata["groups"][i]
                try
                    self.geoserver.delete_user_from_group(user,group)
                    if not self.user_in_group(user,group):
                        del userdata["groups"][i]

                        pos = self._resources["usergroups"][group]["users"].index(user)
                        del self._resources["usergroups"][group]["users"][pos]

                        self._update_checklist("user",operation,[True,"Succeed to delete the user '{}' from usergroup '{}'".format(user,group)])
                        self._update_checklist("user",operation,self.post_delete_user_from_group(user,group))
                    else:
                        self._update_checklist("user",operation,[False,"Failed to delete the user '{}' from usergroup '{}'.".format(user,group)])
                except Exception as ex:
                    self._update_checklist("user",operation,[False,"Failed to delete the user '{}' from usergroup '{}'. {}".format(user,group,ex)])

    def post_delete_user_from_group(self,user,group):
        return None

    def create_role(self):
        role = "role4compatibilitycheck"
        try:
            self.geoserver.add_role(role)
            if self.has_role(role):
                self._set_resource({},"roles",role)
                self._update_checklist("role","create",[True,"Succeed to create the role '{}'.".format(role)])
                self._update_checklist("role","create",self.post_create_role(role))
            else:
                self._update_checklist("role","create",[False,"Failed to create the role '{}'.".format(role)])
        except Exception as ex:
            self._update_checklist("role","create",[False,"Failed to create the role '{}'. {}".format(role,ex)])

    def post_create_role(self,role):
        return None

    def delete_role(self):
        for role in self._resources.get("roles",{}).keys():
            try:
                self.geoserver.delete_role(role)
                if self.geoserver.has_role(role):
                    self._update_checklist("role","delete",[False,"Failed to delete the role '{}'".format(role)])
                else:
                    self._update_checklist("role","delete",[True,"Succeed to delete the role '{}'".format(role)])
                    self._update_checklist("role","delete",self.post_delete_role(role))
            except Exception as ex:
                self._update_checklist("role","delete",[False,"Failed to delete the role '{}'. {}".format(role,ex)])

    def post_delete_role(self,role):
        return  None

    def associate_role_with_user(self):
        operation = "associate role"
        for user,userdata in self._resources.get("users",{}).items():
            for role,roledata in self._resources.get("roles",{}).items():
                try
                    self.geoserver.associate_role_with_user(role,user)
                    if self.user_has_role(user,role):
                        self._update_checklist("user",operation,[False,"Failed to associate the role '{}' with the user '{}'.".format(role,user)])
                    else:
                        if "roles" in userdata:
                            userdata["roles"].append(role)
                        else:
                            userdata["roles"] = [role]
                        if "users" in roledata:
                            roledata["users"].append(user)
                        else:
                            roledata["users"] = [user]
                        self._update_checklist("user",operation,[True,"Succeed to associate the role '{}' with the user '{}'.".format(role,user)])
                        self._update_checklist("user",operation,self.post_associate_role_with_user(role,user))
                except Exception as ex:
                    self._update_checklist("user",operation,[False,"Failed to associate the role '{}' with the user '{}'. {}".format(role,user,ex)])

    def post_associate_role_with_user(self,role,user):
        return None

    def unassociate_role_with_user(self):
        operation = "unassociate role"
        for user,userdata in self._resources.get("users",{}).items():
            if not userdata.get("roles"):
                continue
            for i in range(len(userdata["roles"]) - 1,-1,-1 ):
                role = userdata["roles"][i]
                try
                    self.geoserver.unassociate_role_with_user(role,user)
                    if not self.user_has_role(user,role):
                        del userdata["roles"][i]

                        pos = self._resources["roles"][role]["users"].index(user)
                        del self._resources["roles"][role]["users"][pos]

                        self._update_checklist("user",operation,[True,"Succeed to unassociate the role '{}' with the user '{}'.".format(role,user)])
                        self._update_checklist("user",operation,self.post_unassociate_role_with_user(role,user))
                    else:
                        self._update_checklist("user",operation,[False,"Failed to unassociate the role '{}' with the user '{}'.".format(role,user)])
                except Exception as ex:
                    self._update_checklist("user",operation,[False,"Failed to unassociate the role '{}' with the user '{}'. {}".format(role,user,ex)])

    def post_unassociate_role_with_user(self,role,user):
        return None

    def associate_role_with_usergroup(self):
        operation = "associate role"
        for group,groupdata in self._resources.get("usergroups",{}).items():
            for role,roledata in self._resources.get("roles",{}).items():
                try
                    self.geoserver.associate_role_with_usergroup(role,group)
                    if self.usergroup_has_role(usergroup,role):
                        self._update_checklist("user",operation,[False,"Failed to associate the role '{}' with the usergroup '{}'.".format(role,group)])
                    else:
                        if "roles" in groupdata:
                            groupdata["roles"].append(role)
                        else:
                            groupdata["roles"] = [role]
                        if "usergroups" in roledata:
                            roledata["usergroups"].append(group)
                        else:
                            roledata["usergroups"] = [group]
                        self._update_checklist("user",operation,[True,"Succeed to associate the role '{}' with the usergroup '{}'.".format(role,group)])
                        self._update_checklist("user",operation,self.post_associate_role_with_usergroup(role,group))
                except Exception as ex:
                    self._update_checklist("user",operation,[False,"Failed to associate the role '{}' with the usergroup '{}'. {}".format(role,group,ex)])

    def post_associate_role_with_usergroup(self,role,group):
        return None

    def unassociate_role_with_usergroup(self):
        operation = "unassociate role"
        for group,groupdata in self._resources.get("usergroups",{}).items():
            if not groupdata.get("roles"):
                continue
            for i in range(len(groupdata["roles"]) - 1,-1,-1 ):
                role = groupdata["roles"][i]
                try
                    self.geoserver.unassociate_role_with_usergroup(role,group)
                    if not self.usergroup_has_role(group,role):
                        del groupdata["roles"][i]

                        pos = self._resources["roles"][role]["usergroups"].index(group)
                        del self._resources["roles"][role]["usergroups"][pos]

                        self._update_checklist("user",operation,[True,"Succeed to unassociate the role '{}' with the usergroup '{}'.".format(role,group)])
                        self._update_checklist("user",operation,self.post_unassociate_role_with_usergroup(role,group))
                    else:
                        self._update_checklist("user",operation,[False,"Failed to unassociate the role '{}' with the usergroup '{}'.".format(role,group)])
                except Exception as ex:
                    self._update_checklist("user",operation,[False,"Failed to unassociate the role '{}' with the usergroup '{}'. {}".format(role,group,ex)])

    def post_unassociate_role_with_usergroup(self,role,group):
        return None

    def grant_layer_access_permission(self):
        operation = "grant"
        access_rules = {}
        for wsname,wsdata in self._resources.get("workspaces",{}).items():
            roles = [role for role in self._resources.get("roles",{}).keys()]
            if not roles:
                continue
            access_rules["{}.*.r".format(wsname)] = roles

        if not access_rules:
            return
        try:
            original_rules = self.geoserver.get_layer_access_rules()
            self.geoserver.patch_layer_access_rules(access_rules)
            original_rules.update(access_rules)
            latest_rules = self.geoserver.get_layer_access_rules()
            if original_rules == latest_rules:
                self._set_resource(access_rules,"layer_access_rules")
                self._update_checklist("layer_access_permission",operation,[True,"Succeed to grant the layer access permission {}.".format(access_rules)])
                self._update_checklist("layer_access_permission",self.post_grant_layer_access_permission(access_rules))
            else:
                self._update_checklist("layer_access_permission",operation,[False,"Failed to grant the layer access permission {}.".format(access_rules)])
        except Exception as ex:
            self._update_checklist("layer_access_permission",operation,[False,"Failed to grant the layer access permission {}. {}".format(access_rules,ex)])


    def post_grant_layer_access_permission(self,access_rules):
        return None

    def revoke_layer_access_permission(self):
        operation = "revoke"
        access_rules = self._resources.get("layer_access_rules")
        if not access_rules:
            continue
        delete_permissions = [r for r in access_rules.keys()]
        try:
            original_rules = self.geoserver.get_layer_access_rules()
            for p in delete_permissions:
                del original_rules[p]
            self.geoserver.patch_layer_access_rules(delete_permissions=delete_permissions)
            latest_rules = self.geoserver.get_layer_access_rules()
            if original_rules == latest_rules:
                self._update_checklist("layer_access_permission",operation,[True,"Succeed to revoke the layer access permission {}.".format(access_rules)])
                self._update_checklist("layer_access_permission",self.post_revoke_layer_access_permission(delete_permissions))
            else:
                self._update_checklist("layer_access_permission",operation,[False,"Failed to revoke the layer access permission {}.".format(access_rules)])
        except Exception as ex:
            self._update_checklist("layer_access_permission",operation,[False,"Failed to revoke the layer access permission {}. {}".format(access_rules,ex)])
        


    def post_revoke_layer_access_permission(self,delete_permissions):
        return None

            
if __name__ == '__main__':
    geoserver_url = os.environ["GEOSERVER_URL"]
    geoserver_user = os.environ["GEOSERVER_USER"]
    geoserver_password = os.environ["GEOSERVER_PASSWORD"]

    compatibilitycheck = GeoserverCompatibilityCheck(geoserver_url,geoserver_user,geoserver_password,settings.REQUEST_HEADERS)
    compatibilitycheck.run()


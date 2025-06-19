import os
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
        if resource not in self._checklist:
            self._checklist[resource] = OrderedDict()

        if operation in self._checklist[resource]:
            self._checklist[resource][operation][0][0] = self._checklist[resource][operation][0][0] and status[0]
            if isinstance(self._checklist[resource][operation][0][1],list):
                self._checklist[resource][operation][0][1].append(status[1])
            else:
                self._checklist[resource][operation][0][1] = [self._checklist[resource][operation][0][1],status[1]]
        else:
            self._checklist[resource][operation] = [status]

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
                self._update_checklist("workspace","create"],[True,"Succeed to create the workspace '{}'".format(wsname)])
                self._add_container_resource("workspaces",wsname)
                self.post_create_workspace(wsname)
                return True
            else:
                self._update_checklist("workspace","create"] ,[False,"Failed to create workspace '{}'".format(wsname)])
                return False
        except Exception as ex:
            self._update_checklist("workspace","create"] ,[False,"Failed to create workspace '{}'. {}".format(wsname,ex)])
            return False

    def post_create_workspace(self,wsname):
        """
        called after workspace creation.
        """
        pass

    def delete_workspace(self):
        for wsname in self._resources.get("workspaces",{}).keys():
            if wsname == "__parameters__":
                continue
            try:
                self.geoserver.delete_workspace(wsname)
                if self.geoserver.has_workspace(wsname):
                    self._update_checklist("workspace","delete",[False,"Failed to delete workspace '{}'".format(wsname)])
                    self.post_delete_workspace(wsname)
                else:
                    self._update_checklist("workspace","delete",[True,"Succeed to delete workspace '{}'".format(wsname)])
            except Exception as ex:
                self._update_checklist("workspace","delete"],[False,"Failed to delete workspace '{}'.{}".format(wsname,ex)])

    def post_delete_workspace(self,wsname):
        """
        called after workspace delete.
        """
        pass

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
        try:
            self.geoserver.upload_dataset(wsname,storename,dataset)
            if self.geoserver.has_datastore(wsname,storename):
                self._add_container_resource("workspaces",wsname,"datastores",storename,parameters=dataset)
                self._update_checklist("localdatastore","create"],[True,"Succeed to create the datastore '{}.{}' from local dataset({})".format(wsname,storename,dataset)])
                self.post_create_datastore(wsname,storename,dataset)
                return (storename,dataset)
            else:
                self._update_checklist("localdatastore","create"],[False,"Failed to create the datastore '{}.{}' from local dataset({})".format(wsname,storename,dataset)])
                return None
        except Exception as ex:
            self._update_checklist("localdatastore","create"],[False,"Failed to create the datastore '{}.{}' from local dataset({}). {}".format(wsname,storename,dataset,ex)])
            return None

    def post_create_localdatastore(self,wsname,storename,dataset):
        """
        Called after datastore creation
        """
        pass

    def delete_localdatastore(self):
        for wsname,wsdata in self._resources.get("workspaces",{}).items():
            if wsname == "__parameters__":
                continue
            for storename in wsdate.get("datastores",{}).keys():
                if storename == "__parameters__":
                    continue
                if not storename.startwith("localds"):
                    continue
                try:
                    self.geoserver.delete_datastore(wsname,storename)
                    if self.geoserver.has_datastore(wsname,storename):
                        self._update_checklist("localdatastore","delete",[False,"Failed to delete datastore with local dataset '{}.{}'".format(wsname,storename)])
                    else:
                        self._update_checklist("localdatastore","delete",[True,"Succeed to delete datastore with local dataset '{}.{}'".format(wsname,storename)])
                        self.post_delete_datastore(wsname,storename)
                except Exception as ex:
                    self._update_checklist("localdatastore","delete",[False,"Failed to delete datastore with local dataset '{}.{}. {}'".format(wsname,storename,ex)])
    
    def post_delete_datastore(self,wsname,storename):
        pass

    def create_postgisdatastore(self,wsname):

        """
        create the test workspace if doesn't have
        Return the created datastore. Return None if postgis is not configured, or create failed
        """
        if any(False if os.environ.get(key) else True for key in ("POSTGIS_HOST","POSTGIS_PORT","POSTGIS_DATABASE","POSTGIS_USER")):
            #test postgis datastore disabled
            return None

        storename = "postgisds4compatibilitycheck"
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
                self._add_container_resource("workspaces",wsname,"datastores",storename)
                self._update_checklist("postgisdatastore","create"],[True,"Succeed to create the datastore '{}.{}' for postgis db".format(wsname,storename)])
                self.post_create_datastore(wsname,storename,parameters)
                return storename
            else:
                self._update_checklist("postgisdatastore","create"],[False,"Failed to create the datastore '{}.{}' for postgis db".format(wsname,storename)])
                return None
        except Exception as ex:
            self._update_checklist("postgisdatastore","create"],[False,"Failed to create the datastore '{}.{}' for postgis db. {}".format(wsname,storename,ex)])
            return None

    def post_create_postgisdatastore(self,wsname,storename,parameters):
        pass

    def publish_featuretype_from_localdataset(self,wsname,storename,dataset):
        """
        Publish featuretype from local dataset
        Return layername if succeed; otherwise return None
        """
        layername = os.path.splitext(os.path.basename(dataset))[0]
        parameters = {"nativeName":layername}
        try:
            self.geoserver.publish_featuretype(wsname,storename,layername,parameters,create=True)
            if has_featuretype(wsname,layername,storename):
                self._add_container_resource("workspaces",wsname,"datastores",storename,layername)
                self._update_checklist("localfeaturetype","create"],[True,"Succeed to create the featuretype '{}.{}.{}' from local dataset".format(wsname,storename,layername)])
                self.post_publish_featuretype(wsname,storename,layername,parameers)
                return layernname
            else:
                self._update_checklist("localfeaturetype","create"],[False,"Failed to create the featuretype '{}.{}.{}' from local dataset".format(wsname,storename,layername)])
                return None
        except Exception as ex:
            self._update_checklist("localfeaturetype","create"],[False,"Failed to create the featuretype '{}.{}.{}' from local dataset. {}".format(wsname,storename,layername,ex)])
            return None

    def post_publish_feturetype(self,wsname,storename,layername,parameters):
        pass

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
                    self._append_resource(None,"workspaces",wsname,"styles",layername,stylename)
                    self._update_checklist("style","create",[True,"Succeed to create the style '{}.{}'".format(wsname,stylename)])
                    self.post_create_style(wsname,stylename,styleversion,styledata)
                else:
                    self._update_checklist("style","create",[False,"Failed to create the style '{}.{}'".format(wsname,stylename)])
            except Exception as ex:
                self._update_checklist("style","create",[False,"Failed to create the style '{}.{}'. {}".format(wsname,stylename,ex)])

    def post_create_style(self,wsname,stylename,styleversion,styledata):
        pass


    def publish_featuretype_from_postgis(self,wsname,storename):
        """
        Publish the feature type from postgis
        return the published layes if published; otherwise return empty list

        """
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
                    self._add_container_resource("workspaces",wsname,"datastores",storename,layername)
                    self._update_checklist("postgisfeaturetype","create"],[True,"Succeed to publish the featuretype '{}.{}.{}' from postgis table".format(wsname,storename,layername)])
                    published_layers.append(layername)
                    self.post_publish_featuretype(wsname,storename,layername,parameters)
                else:
                    self._update_checklist("postgisfeaturetype","create"],[False,"Failed to publish the featuretype '{}.{}.{}' from postgis table".format(wsname,storename,layername)])
            except Exception as ex:
                self._update_checklist("postgisfeaturetype","create"],[False,"Failed to publish the featuretype '{}.{}.{}' from postgis table. {}".format(wsname,storename,layername,ex)])

        return published_layers


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
                self._add_container_resource("workspaces",wsname,"wmsstores",storename)
                self._update_checklist("wmsstore","create"],[True,"Succeed to create the wmsstore '{}.{}'".format(wsname,storename)])
                self.post_create_wmsstore(wsname,storename,parameters)
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
        pass

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
                            self._add_container_resource("workspaces",wsname,"wmsstores",storename,layername)
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
    
    def create_layergroup(self,wsname):
        """
        Create a layergroup 
        Return the layergroup if succeed;otherwise return None
        """
        try:
            parameters={"layers":[]}
            groupname="layergroup4compatibilitycheck"
            for workspace,workspacedata in self._resource.get("workspaces").item():
                for dsname, dsdata in workspacedata.get("datastores",[]).items():
                    for layername in dsdata.keys():
                        try:
                            parameters["layers"].append({"type":"layer","name":layername,"workspace":workspace})

            if parameters["layers"]:
                self.geoserver.update_layergroup(wsname,groupname,parameters):
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

    def post_create_layergroup(self,wsname,groupname,parameters):
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
                    self.geoserver.update_layergroup(wsname,groupname,parameters):
                    if self.geoserver.has_layergroup(wsname,groupname):
                        self._update_checklist("layergroup","update",[True,"Succeed to update layergroup '{}.{}'".format(wsname,groupname)])
                        self.post_update_layergroup(self,groupname,parameters)
                    else:
                        self._update_checklist("layergroup","update",[False,"Failed to update layergroup '{}.{}'".format(wsname,groupname)])
                except Exception as ex:
                    self._update_checklist("layergroup","update",[False,"Failed to update layergroup '{}.{}'. {}".format(wsname,groupname,ex)])

    def post_update_layergroup(self,groupname,parameters):
        pass

    def delete_layergroup(self):
        for wsname,wsdata in self._resources.get("workspaces",{}).items():
            if wsname == "__parameters__":
                continue
            for groupname in wsdate.get("layergroups",[]):
                if groupname == "__parameters__":
                    continue
                try:
                    self.geoserver.delete_layergroup(wsname,groupname)
                    if self.geoserver.has_layergroup(wsname,groupname):
                        self._update_checklist("layergroup","delete",[False,"Failed to delete layergroup '{}.{}'".format(wsname,groupname)])
                    else:
                        self._update_checklist("layergroup","delete",[True,"Succeed to delete layergroup '{}.{}'".format(wsname,groupname)])
                        self.post_delete_layergroup(wsname,groupname)
                except Exception as ex:
                    self._update_checklist("layergroup","delete",[False,"Failed to delete layergroup '{}.{}'. {}".format(wsname,groupname,ex)])

    def post_delete_layergroup(self,wsname,groupname):
        pass

    def delete_wmslayers(self):
        pass

    def update_styles(self):
        pass

    def delete_style(self):
        for wsname,wsdata in self._resources.get("workspaces",{}).items():
            if wsname == "__parameters__":
                continue
            for layername,styles in wsdate.get("styles",{}).items():
                if layername == "__parameters__":
                    continue
                for stylename in styles:
                    if stylename == "__parameters__":
                        continue
                    try:
                        self.geoserver.delete_style(wsname,stylename)
                        if self.geoserver.has_style(wsname,stylename):
                            self._update_checklist("style","delete",[False,"Failed to delete style '{}.{}'".format(wsname,stylename)])
                        else:
                            self._update_checklist("style","delete",[True,"Succeed to delete style '{}.{}'".format(wsname,stylename)])
                    except Exception as ex:
                        self._update_checklist("style","delete",[False,"Failed to delete style '{}.{}'. {}".format(wsname,stylename,ex)])

    def update_featuretype_style(self):
        pass

    def update_postgis_datastore(self):
        pass

    def delete_postgis_datastore(self):
        for wsname,wsdata in self._resources.get("workspaces",{}).items():
            if wsname == "__parameters__":
                continue
            for storename in wsdate.get("datastores",{}).keys():
                if storename == "__parameters__":
                    continue
                if not storename.startwith("postgisds"):
                    continue
                try:
                    self.geoserver.delete_datastore(wsname,storename)
                    if self.geoserver.has_datastore(wsname,storename):
                        self._update_checklist("localdatastore","delete",[False,"Failed to delete postgis datastore '{}.{}'".format(wsname,storename)])
                    else:
                        self._update_checklist("localdatastore","delete",[True,"Succeed to delete postgis datastore '{}.{}'".format(wsname,storename)])
                except Exception as ex:
                    self._update_checklist("localdatastore","delete",[False,"Failed to delete postgis datastore '{}.{}. {}'".format(wsname,storename,ex)])


    def update_featuretype(self):
        pass

    def delete_localfeaturetype(self):
        for wsname,wsdata in self._resources.get("workspaces",{}).items():
            if wsname == "__parameters__":
                continue
            for storename,storedata in wsdate.get("datastores",{}).items():
                if storename == "__parameters__":
                    continue
                if not storename.startswith("localds")):
                    continue
                for layername in storedata.keys():
                    if layername == "__parameters__":
                        continue
                    try:
                        self.geoserver.delete_featuretype(wsname,storename,layername)
                        if self.geoserver.has_featuretype(wsname,storename,layername):
                            self._update_checklist("localfeaturetype","delete",[False,"Failed to delete featuretype from local dataset '{}.{}.{}'".format(wsname,storename,layername)])
                        else:
                            self._update_checklist("localfeaturetype","delete",[True,"Succeed to delete featuretype from local dataset '{}.{}.{}'".format(wsname,storename,layername)])
                    except Exception as ex:
                        self._update_checklist("localfeaturetype","delete",[False,"Failed to delete featuretype from local dataset '{}.{}.{}'. {}".format(wsname,storename,layername,ex)])

    def delete_postgisfeaturetype(self):
        for wsname,wsdata in self._resources.get("workspaces",{}).items():
            if wsname == "__parameters__":
                continue
            for storename,storedata in wsdate.get("datastores",{}).items():
                if storename == "__parameters__":
                    continue
                if not storename.startswith("postgisds")):
                    continue
                for layername in storedata.keys():
                    if layername == "__parameters__":
                        continue
                    try:
                        self.geoserver.delete_featuretype(wsname,storename,layername)
                        if self.geoserver.has_featuretype(wsname,storename,layername):
                            self._update_checklist("postgisfeaturetype","delete",[False,"Failed to delete featuretype from postgis table '{}.{}.{}'".format(wsname,storename,layername)])
                        else:
                            self._update_checklist("postgisfeaturetype","delete",[True,"Succeed to delete featuretype from postgis table '{}.{}.{}'".format(wsname,storename,layername)])
                    except Exception as ex:
                        self._update_checklist("postgisfeaturetype","delete",[False,"Failed to delete featuretype from postgis table '{}.{}.{}'. {}".format(wsname,storename,layername,ex)])

    def create_wmtslayer(self):
        pass

    def update_wmtslayer(self):
        pass

    def check_wmts_expire(self):
        pass

    def check_wmts_clear_cache(self):
        pass

    def delete_wmtslayer(self):
        pass

    def create_user(self):
        users=["user4compatibilitycheck{}@test.com".format(i) for i in range(1,3,1)]
        for user in users:
            self.geoserver.update_user(user,"1234",,enabled=True,create=True)
            

    def enable_user(self):
        pass

    def update_userpassword(self):
        pass

    def delete_uesr(self):
        for user in self._resources["users"]:
            try
                self.geoserver.delete_user(user)
                if self.has_user(user):
                    self._update_checklist("user","delete",[False,"Failed to delete user '{}'".format(user)])
                else:
                    self._update_checklist("user","delete",[True,"Succeed to delete user '{}'".format(user)])
            except Exception as ex:
                self._update_checklist("user","delete",[False,"Failed to delete user '{}'. {}".format(user,ex)])

    def create_usergroup(self):
        pass

    def add_user_to_group(self):
        pass

    def remote_user_from_group(self):
        pass

    def delete_usergroup(self):
        for usergroup in self._resources["usergroups"]:
            try
                self.geoserver.delete_usergroup(usergroup)
                if self.has_usegroupr(usergroup):
                    self._update_checklist("usergroup","delete",[False,"Failed to delete user group '{}'".format(usergroup)])
                else:
                    self._update_checklist("usergroup","delete",[True,"Succeed to delete user group '{}'".format(usergroup)])
            except Exception as ex:
                self._update_checklist("usergroup","delete",[False,"Failed to delete user group '{}'. {}".format(usergroup,ex)])

    def update_access_rules(self):
        pass

    def set_catalogue_mode(self):
        pass


            
if __name__ == '__main__':
    geoserver_url = os.environ["GEOSERVER_URL"]
    geoserver_user = os.environ["GEOSERVER_USER"]
    geoserver_password = os.environ["GEOSERVER_PASSWORD"]

    compatibilitycheck = GeoserverCompatibilityCheck(geoserver_url,geoserver_user,geoserver_password,settings.REQUEST_HEADERS)
    compatibilitycheck.run()


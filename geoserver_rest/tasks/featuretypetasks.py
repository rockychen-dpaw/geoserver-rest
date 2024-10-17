import json
import os
import logging

from .base import Task
from .. import settings
from .datastoretasks import ListDatastores
from .workspacetasks import ListResourcesInWorkspace

logger = logging.getLogger(__name__)

class WFSGetCapabilitiesTask(Task):
    category = "Get Capabilities"
    arguments = ("service",)
    keyarguments = ("service",)
    service = "WFS"
    url = None
    def _format_result(self):
        return "URL : {}\r\nCapabilities File Size = {}".format(self.url or "",self.result)

    def _exec(self,geoserver):
        self.url = geoserver.wfscapabilities_url()
        file = geoserver.get_wfscapabilities()
        try:
            return os.path.getsize(file)
        finally:
            try:
                os.remove(file)
            except:
                logger.error("Failed to delete temporary file '{}'".format(file))
                pass


class ListFeatureTypes(Task):
    """
    Return [featuretype]
    """
    arguments = ("workspace","datastore")
    keyarguments = ("workspace","datastore")
    category = "List Features"
    def __init__(self,workspace,datastore,post_actions_factory = None):
        super().__init__(post_actions_factory = post_actions_factory) 
        self.workspace = workspace
        self.datastore = datastore

    def _format_result(self):
        return "FeatureTypes = {}".format(len(self.result) if self.result else 0) 

    def _exec(self,geoserver):
        result = geoserver.list_featuretypes(self.workspace,storename=self.datastore) or []
        result.sort()
        return result

    def _warnings(self):
        if not settings.IGNORE_EMPTY_DATASTORE and not self.result:
            yield (self.WARNING,"The datastore({}:{}) is empty.".format(self.workspace,self.datastore))

class GetFeatureTypeDetail(Task):
    """
    Return a dict of feature type detail
    """
    arguments = ("workspace","datastore","featuretype")
    keyarguments = ("workspace","datastore","featuretype")
    category = "Get Featuretype Detail "

    def __init__(self,workspace,datastore,featuretype,post_actions_factory = None):
        super().__init__(post_actions_factory = post_actions_factory) 
        self.workspace = workspace
        self.datastore = datastore
        self.featuretype = featuretype

    def _warnings(self):
        if not self.result:
            yield (self.ERROR,"Detail is missing")
        msg = None
        level = self.WARNING
        if self.result.get("gwc"):
            for gridset in settings.GWC_GRIDSETS:
                if not any(gridsetdata["gridSetName"] == gridset  for gridsetdata in self.result["gwc"]["gridSubsets"]):
                    msg = "{}{}{}".format(msg or "", "\r\n" if msg else "","The gridset({}) was not configured".format(gridset))
            if not self.result["gwc"]["enabled"]:
                msg = "{}{}{}".format(msg or "", "\r\n" if msg else "","The GWC was disabled.")
            if self.result["gwc"].get("expireCache",0) < 0:
                msg = "{}{}{}".format(msg or "", "\r\n" if msg else "","The GWC server cache was disabled.")
                level = self.ERROR
            if self.result["gwc"].get("expireClients",0) > settings.MAX_EXPIRE_CLIENTS:
                msg = "{}{}{}".format(msg or "", "\r\n" if msg else "","The GWC client cache is greater than {} seconds".format(settings.MAX_EXPIRE_CLIENTS_STR))

        if msg:
            yield (level,msg)

    def _format_result(self):
        return json.dumps(self.result,indent=4) if self.result else "{}"

    def _exec(self,geoserver):
        result = {"geometry":None}
        #get the feature details
        detail = geoserver.get_featuretype(self.workspace,self.featuretype)
        for k in ["nativeName","title","abstract","srs","nativeBoundingBox","latLonBoundingBox","enabled","attributes"]:
            if not detail.get(k):
                continue
            if k == "attributes":
                result[k] = []
                for attr in detail[k].get("attribute") or []:
                    result[k].append({})
                    for n in ["name","nillable","binding"]:
                        if n == "binding":
                            if attr[n].startswith("org.locationtech.jts.geom."):
                                result["geometry"] = attr[n].rsplit(".",1)[1]

                        result[k][-1][n] = attr[n]

                continue
            result[k] = detail[k]
        #get the feature styles
        styles = geoserver.get_layer_styles(self.workspace,self.featuretype)
        result["defaultStyle"] = (":".join(styles[0]) if styles[0][0] else styles[0][1]) if styles else None
        result["alternativeStyles"] = [("{}:{}".format(w,style) if w else style)  for w,style in styles[1]] if styles and styles[1] else []
        result["alternativeStyles"].sort()
        #get the gwc details
        detail = geoserver.get_gwclayer(self.workspace,self.featuretype)
        if detail:
            result["gwc"] = {}
            for k in ["expireClients","expireCache","gridSubsets","enabled"]:
                if k in detail:
                    result["gwc"][k] = detail[k]
        
        return result

class GetFeatureCount(Task):
    """
    Return a dict of feature type detail
    """
    arguments = ("workspace","datastore","featuretype")
    keyarguments = ("workspace","datastore","featuretype")
    category = "Get Feature Count"

    def __init__(self,workspace,datastore,featuretype,post_actions_factory = None):
        super().__init__(post_actions_factory = post_actions_factory) 
        self.workspace = workspace
        self.datastore = datastore
        self.featuretype = featuretype

    def _format_result(self):
        return "Features = {}".format(self.result if self.result else 0)

    def _exec(self,geoserver):
        return geoserver.get_featurecount(self.workspace,self.featuretype)

class GetFeatures(Task):
    """
    Return a number of features in geojson format
    """
    arguments = ("workspace","datastore","featuretype","srs","bbox","count")
    keyarguments = ("workspace","datastore","featuretype","srs")
    category = "Get Features"
    url = None

    def __init__(self,workspace,datastore,featuretype,featuredetail,post_actions_factory = None):
        super().__init__(post_actions_factory = post_actions_factory) 
        self.workspace = workspace
        self.datastore = datastore
        self.featuretype = featuretype
        self.featuredetail = featuredetail

    @property
    def srs(self):
       return self.featuredetail.get("srs")

    @property
    def bbox(self):
       return [self.featuredetail["latLonBoundingBox"][k] for k in ("minx","miny","maxx","maxy")]

    @property
    def count(self):
        return settings.TEST_FEATURES_COUNT

    def _format_result(self):
        return "URL : {} \r\nTotal Features : {} , Matched Features : {} , Returned Features : {} , BBOX : {}".format(
            self.url or "",
            self.result.get("totalFeatures",0) if self.result else 0,
            self.result.get("numberMatched",0) if self.result else 0,
            self.result.get("numberReturned",0) if self.result else 0,
            ",".join(str(d) for d in self.result["bbox"]) if self.result and self.result.get("bbox") else ""
            )

    def _exec(self,geoserver):
        if self.featuredetail["geometry"]:
            self.url = geoserver.features_url(
                self.workspace,
                self.featuretype,
                count=self.count,
                srs=self.srs,
                bbox=self.bbox
            )
            return geoserver.get_features(
                self.workspace,
                self.featuretype,
                storename=self.datastore,
                count=self.count,
                srs=self.srs,
                bbox=self.bbox
            )
        else:
            self.url = geoserver.features_url(
                self.workspace,
                self.featuretype,
                count=self.count
            )
            return geoserver.get_features(
                self.workspace,
                self.featuretype,
                storename=self.datastore,
                count=self.count
            )

def createtasks_ListFeatureTypes(task,limit = 0):
    """
    a generator to return featuretypes tasks
    """
    if isinstance(task,ListDatastores):
        result = task.result
    elif isinstance(task,ListResourcesInWorkspace):
        result = (task.result[0] or []) if task.result else []
    else:
        return

    if not result:
        return
    row = 0
    for store in result:
        row += 1
        if limit > 0 and row > limit:
            break
        yield ListFeatureTypes(task.workspace,store,post_actions_factory=task.post_actions_factory)


def createtasks_GetFeatureTypeDetail(listFeatureTypesTask,limit = 0):
    """
    a generator to return featuretype styles tasks
    """
    if not listFeatureTypesTask.result:
        return
    row = 0
    for featuretype in listFeatureTypesTask.result:
        row += 1
        if limit > 0 and row > limit:
            break
        yield GetFeatureTypeDetail(listFeatureTypesTask.workspace,listFeatureTypesTask.datastore,featuretype,post_actions_factory=listFeatureTypesTask.post_actions_factory)


def createtasks_GetFeatureCount(listFeatureTypesTask,limit = 0):
    """
    a generator to return featuretype styles tasks
    """
    if not listFeatureTypesTask.result:
        return
    row = 0
    for featuretype in listFeatureTypesTask.result:
        row += 1
        if limit > 0 and row > limit:
            break
        yield GetFeatureCount(listFeatureTypesTask.workspace,listFeatureTypesTask.datastore,featuretype,post_actions_factory=listFeatureTypesTask.post_actions_factory)


def createtasks_GetFeatures(getFeatureTypeDetailTask,limit = 0):
    """
    a generator to return featuretype styles tasks
    """
    if not getFeatureTypeDetailTask.result:
        return
    layer_bbox = getFeatureTypeDetailTask.result.get("latLonBoundingBox")
    if not layer_bbox or any(layer_bbox.get(k) is None for k in ("minx","miny","maxx","maxy")):
        return
    
    yield GetFeatures(
        getFeatureTypeDetailTask.workspace,
        getFeatureTypeDetailTask.datastore,
        getFeatureTypeDetailTask.featuretype,
        getFeatureTypeDetailTask.result,
        post_actions_factory=getFeatureTypeDetailTask.post_actions_factory
    )


def createtasks_WFSGetCapabilities(task,limit = 0):
    """
    a generator to return WFSGetCapabilitiesTask
    """
    if not task.is_succeed:
        return
    yield WFSGetCapabilitiesTask(
        post_actions_factory=task.post_actions_factory
    )



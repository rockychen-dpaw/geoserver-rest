import json
import logging
import os
from pyproj import Transformer

from .base import Task
from .. import timezone
from .. import settings
from .wmsstoretasks import ListWMSStores
from .workspacetasks import ListResourcesInWorkspace

logger = logging.getLogger(__name__)

class WMSGetCapabilitiesTask(Task):
    category = "Get Capabilities"
    arguments = ("service",)
    keyarguments = ("service",)
    service = "WMS"
    url = None

    def _format_result(self):
        return "URL : {}\r\nCapabilities File Size = {}".format(self.url or "",self.result)

    def _exec(self,geoserver):
        self.url = geoserver.wmscapabilities_url()
        file = geoserver.get_wmscapabilities()
        try:
            return os.path.getsize(file)
        finally:
            try:
                os.remove(file)
            except:
                logger.error("Failed to delete temporary file '{}'".format(file))
                pass

class ListWMSLayers(Task):
    """
    Return [wmslayer]
    """
    arguments = ("workspace","wmsstore")
    keyarguments = ("workspace","wmsstore")
    category = "List WMSLayers"
    def __init__(self,workspace,wmsstore,post_actions_factory = None):
        super().__init__(post_actions_factory = post_actions_factory) 
        self.workspace = workspace
        self.wmsstore = wmsstore

    def _format_result(self):
        return "WMSLayers : {}".format(len(self.result) if self.result else 0) 

    def _warnings(self):
        if not settings.IGNORE_EMPTY_WMSSTORE and not self.result:
            yield (self.WARNING,"The WMSstore({}:{}) is empty.".format(self.workspace,self.wmsstore))

    def _exec(self,geoserver):
        result = geoserver.list_wmslayers(self.workspace,self.wmsstore) or []
        if self.workspace in settings.EXCLUDED_LAYERS:
            for i in range(len(result) - 1,-1,-1):
                if result[i] in settings.EXCLUDED_LAYERS[self.workspace]:
                    #excluded
                    del result[i]
        result.sort()
        return result
        
class GetWMSLayerDetail(Task):
    """
    Return a dict of wms layer detail
    """
    arguments = ("workspace","wmsstore","layername")
    keyarguments = ("workspace","wmsstore","layername")
    category = "Get WMSLayer Detail "

    def __init__(self,workspace,wmsstore,layername,post_actions_factory = None):
        super().__init__(post_actions_factory = post_actions_factory) 
        self.workspace = workspace
        self.wmsstore = wmsstore
        self.layername = layername

    def _format_result(self):
        return json.dumps(self.result,indent=4) if self.result else "{}"

    def _warnings(self):
        msg = None
        level = self.WARNING
        if self.result.get("originalLatLonBoundingBox") and self.result["originalLatLonBoundingBox"].get("crs","EPSG:4326").upper() not in ("EPSG:4326","EPSG:4283"):
            msg = "The CRS of latLonBoundingBox is not EPSG:4326 or EPSG:4283"
        
        if msg:
            yield (level,msg)

    def _exec(self,geoserver):
        result = {}
        #get the layer detail
        detail = geoserver.get_wmslayer(self.workspace,self.layername)
        for k in ["nativeName","title","abstract","srs","nativeBoundingBox","latLonBoundingBox","enabled"]:
            if not detail.get(k):
                continue
            result[k] = detail[k]

        if result.get("latLonBoundingBox") and result["latLonBoundingBox"].get("crs","EPSG:4326").upper() not in ("EPSG:4326","EPSG:4283"):
            #tranform the bbox to epsg:4326
            result["originalLatLonBoundingBox"] = dict(result["latLonBoundingBox"])
            transformer = Transformer.from_crs(result["latLonBoundingBox"]["crs"], "EPSG:4326")
            result["latLonBoundingBox"]["miny"], result["latLonBoundingBox"]["minx"] = transformer.transform(result["latLonBoundingBox"]["miny"], result["latLonBoundingBox"]["minx"])
            result["latLonBoundingBox"]["maxy"], result["latLonBoundingBox"]["maxx"] = transformer.transform(result["latLonBoundingBox"]["maxy"], result["latLonBoundingBox"]["maxx"])

        #get the gwc details
        detail = geoserver.get_gwclayer(self.workspace,self.layername)
        if detail:
            result["gwc"] = {}
            for k in ["expireClients","expireCache","gridSubsets","enabled"]:
                result["gwc"][k] = detail[k]
        
        return result

def createtasks_ListWMSLayers(task,limit = 0):
    """
    a generator to return layernames tasks
    """
    if isinstance(task,ListWMSStores):
        result = task.result
    elif isinstance(task,ListResourcesInWorkspace):
        result = (task.result[1] or []) if task.result else []
    else:
        return
    if not result:
        return
    row = 0
    for store in result:
        row += 1
        if limit > 0 and row > limit:
            break
        yield ListWMSLayers(task.workspace,store,post_actions_factory=task.post_actions_factory)


def createtasks_GetWMSLayerDetail(listWMSLayersTask,limit = 0):
    """
    a generator to return WMSLayer detail tasks
    """
    if not listWMSLayersTask.result:
        return
    row = 0
    for layername in listWMSLayersTask.result:
        row += 1
        if limit > 0 and row > limit:
            break
        yield GetWMSLayerDetail(listWMSLayersTask.workspace,listWMSLayersTask.wmsstore,layername,post_actions_factory=listWMSLayersTask.post_actions_factory)

def createtasks_WMSGetCapabilities(task,limit = 0):
    """
    a generator to return WMSGetCapabilitiesTask
    """
    if not task.is_succeed:
        return
    yield WMSGetCapabilitiesTask(
        post_actions_factory=task.post_actions_factory
    )



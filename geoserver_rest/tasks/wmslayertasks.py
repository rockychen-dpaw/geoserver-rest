import json
import logging
import os

from .base import Task
from .. import timezone
from .. import settings
from .wmsstoretasks import ListWMSstores
from .workspacetasks import ListResourcesInWorkspace
from ..exceptions import *

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
    def __init__(self,workspace,wmsstore,storedetails,post_actions_factory = None):
        super().__init__(post_actions_factory = post_actions_factory) 
        self.workspace = workspace
        self.wmsstore = wmsstore
        self.storedetails = storedetails

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

    def __init__(self,workspace,wmsstore,layername,storedetails,post_actions_factory = None):
        super().__init__(post_actions_factory = post_actions_factory) 
        self.workspace = workspace
        self.wmsstore = wmsstore
        self.layername = layername
        self.storedetails = storedetails

    def _format_result(self):
        return json.dumps(self.result,indent=4) if self.result else "{}"

    @property
    def enabled(self):
        return self.result and self.result.get("enabled") and self.storedetails.get("enabled")

    @property
    def gwcenabled(self):
        return self.enabled and self.result.get("gwc",{}).get("enabled")

    def _warnings(self):
        msg = []
        level = self.WARNING
        if not self.result:
            msg.append("Detail is missing")
            level = self.ERROR

        if not self.result.get("enabled") or not self.storedetails.get("enabled"):
            msg.append("The layer is disabled.")

        if "gwc" in self.result:
            if not self.result["gwc"].get("enabled"):
                msg.append("The GWC is disabled.")

            if self.result["gwc"].get("expireCache",0) < 0:
                msg.append("The GWC server cache is disabled.")
                level = self.ERROR

        if self.result.get("latLonBoundingBox") and self.result["latLonBoundingBox"]["crs"].upper() not in ("EPSG:4326","EPSG:4283"):
            msg.append("The CRS({}) of latLonBoundingBox is not EPSG:4326 or EPSG:4283\r\n{}".format(self.result["latLonBoundingBox"]["crs"],self.result.get("originalLatLonBoundingBox")))
        
        if msg:
            yield (level,"\r\n".join(msg))

    def _exec(self,geoserver):
        result = {}
        #get the layer detail
        detail = geoserver.get_wmslayer(self.workspace,self.layername)
        for k in ["nativeName","title","abstract","srs","nativeBoundingBox","latLonBoundingBox","enabled"]:
            if not detail.get(k):
                continue
            result[k] = detail[k]

        if result.get("latLonBoundingBox"):
            if "crs" not in result["latLonBoundingBox"]:
                result["latLonBoundingBox"]["crs"] = "EPSG:4326"
            elif isinstance(result["latLonBoundingBox"]["crs"],dict):
                result["latLonBoundingBox"]["crs"] = result["latLonBoundingBox"]["crs"]["$"]
 
        #get the gwc details
        detail = None
        try:
            detail = geoserver.get_gwclayer(self.workspace,self.layername)
        except ResourceNotFound as ex:
            pass

        if detail:
            result["gwc"] = {}
            for k in ["expireClients","expireCache","gridSubsets","enabled"]:
                result["gwc"][k] = detail[k]
        
        return result

def createtasks_ListWMSLayers(getWMSStoreTask,limit = 0):
    """
    a generator to return layernames tasks
    """
    if not getWMSStoreTask.result:
        return
    yield ListWMSLayers(getWMSStoreTask.workspace,getWMSStoreTask.wmsstore,getWMSStoreTask.result,post_actions_factory=getWMSStoreTask.post_actions_factory)


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
        yield GetWMSLayerDetail(
            listWMSLayersTask.workspace,
            listWMSLayersTask.wmsstore,
            layername,
            listWMSLayersTask.storedetails,
            post_actions_factory=listWMSLayersTask.post_actions_factory
        )

def createtasks_WMSGetCapabilities(task,limit = 0):
    """
    a generator to return WMSGetCapabilitiesTask
    """
    if not task.is_succeed:
        return
    yield WMSGetCapabilitiesTask(
        post_actions_factory=task.post_actions_factory
    )



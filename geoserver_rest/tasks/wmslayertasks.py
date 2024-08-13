import json

from .base import Task
from .. import timezone

class ListWMSLayers(Task):
    """
    Return [wmslayer]
    """
    arguments = ("workspace","wmsstore")
    category = "List WMSLayers"
    def __init__(self,workspace,wmsstore,post_actions_factory = None):
        super().__init__(post_actions_factory = post_actions_factory) 
        self.workspace = workspace
        self.wmsstore = wmsstore

    def _format_result(self):
        return "WMSLayers : {}".format(len(self.result) if self.result else 0) 

    def _warnings(self):
        if not self.result:
            yield "The WMSstore({}:{}) is empty.".format(self.workspace,self.wmsstore)

    def _exec(self,geoserver):
        return geoserver.list_wmslayers(self.workspace,self.wmsstore)
        
class GetWMSLayerDetail(Task):
    """
    Return a dict of wms layer detail
    """
    arguments = ("workspace","wmsstore","layername")
    category = "Get WMSLayer Detail "

    def __init__(self,workspace,wmsstore,layername,post_actions_factory = None):
        super().__init__(post_actions_factory = post_actions_factory) 
        self.workspace = workspace
        self.wmsstore = wmsstore
        self.layername = layername

    def _format_result(self):
        return json.dumps(self.result,indent=4) if self.result else "{}"

    def _exec(self,geoserver):
        result = {}
        #get the layer detail
        detail = geoserver.get_wmslayer(self.workspace,self.layername)
        for k in ["nativeName","title","abstract","srs","nativeBoundingBox","latLonBoundingBox","enabled"]:
            if not detail.get(k):
                continue
            result[k] = detail[k]
        #get the gwc details
        detail = geoserver.get_gwclayer(self.workspace,self.layername)
        if detail:
            result["gwc"] = {}
            for k in ["expireClients","expireCache","gridSubsets","enabled"]:
                result["gwc"][k] = detail[k]
        
        return result

def createtasks_ListWMSLayers(listWMSstoresTask,limit = 0):
    """
    a generator to return layernames tasks
    """
    if not listWMSstoresTask.result:
        return
    row = 0
    for store in listWMSstoresTask.result:
        row += 1
        if limit > 0 and row > limit:
            break
        yield ListWMSLayers(listWMSstoresTask.workspace,store,post_actions_factory=listWMSstoresTask.post_actions_factory)


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


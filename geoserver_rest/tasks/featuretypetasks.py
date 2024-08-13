import json
import os

from .base import Task
from .. import timezone
from .. import settings

class ListFeatureTypes(Task):
    """
    Return [featuretype]
    """
    arguments = ("workspace","datastore")
    category = "List Features"
    def __init__(self,workspace,datastore,post_actions_factory = None):
        super().__init__(post_actions_factory = post_actions_factory) 
        self.workspace = workspace
        self.datastore = datastore

    def _format_result(self):
        return "FeatureTypes = {}".format(len(self.result) if self.result else 0) 

    def _exec(self,geoserver):
        return geoserver.list_featuretypes(self.workspace,storename=self.datastore) or []

    def _warnings(self):
        if not self.result:
            yield "The datastore({}:{}) is empty.".format(self.workspace,self.datastore)

class GetFeatureTypeDetail(Task):
    """
    Return a dict of feature type detail
    """
    arguments = ("workspace","datastore","featuretype")
    category = "Get Featuretype Detail "

    def __init__(self,workspace,datastore,featuretype,post_actions_factory = None):
        super().__init__(post_actions_factory = post_actions_factory) 
        self.workspace = workspace
        self.datastore = datastore
        self.featuretype = featuretype

    def _warnings(self):
        if not self.result:
            yield "Detail is missing"
        msg = None
        if self.result.get("gwc"):
            for gridset in settings.GWC_GRIDSETS:
                if gridset not in self.result["gwc"]["gridSubsets"]:
                    msg = "{}{}{}".format(msg or "", "\r\n" if msg else "","The gridset({}) was not configured".format(gridset))
            if not self.result["gwc"]["enabled"]:
                msg = "{}{}{}".format(msg or "", "\r\n" if msg else "","The GWC was disabled.")
            if self.result["gwc"].get("expireCache",0) < 0:
                msg = "{}{}{}".format(msg or "", "\r\n" if msg else "","The GWC server cache was disabled.")
            if self.result["gwc"].get("expireClients",0) > settings.MAX_EXPIRE_CLIENTS:
                msg = "{}{}{}".format(msg or "", "\r\n" if msg else "","The GWC client cache is greater than {} seconds".format(settings.MAX_EXPIRE_CLIENTS_STR))

    def _format_result(self):
        return json.dumps(self.result,indent=4) if self.result else "{}"

    def _exec(self,geoserver):
        result = {}
        #get the feature details
        detail = geoserver.get_featuretype(self.workspace,self.featuretype)
        for k in ["nativeName","title","abstract","srs","nativeBoundingBox","latLonBoundingBox","enabled","attributes"]:
            if not detail.get(k):
                continue
            if k == "attributes":
                result[k] = []
                for attr in detail[k]["attribute"]:
                    result[k].append({})
                    for n in ["name","nillable","binding"]:
                        result[k][-1][n] = attr[n]
                continue
            result[k] = detail[k]
        #get the feature styles
        styles = geoserver.get_layer_styles(self.workspace,self.featuretype)
        result["defaultStyle"] = (":".join(styles[0]) if styles[0][0] else styles[0][1]) if styles else None
        result["alternativeStyles"] = [("{}:{}".format(w,style) if w else style)  for w,style in styles[1]] if styles and styles[1] else []
        #get the gwc details
        detail = geoserver.get_gwclayer(self.workspace,self.featuretype)
        if detail:
            result["gwc"] = {}
            for k in ["expireClients","expireCache","gridSubsets","enabled"]:
                result["gwc"][k] = detail[k]
        
        return result

class GetFeatureCount(Task):
    """
    Return a dict of feature type detail
    """
    arguments = ("workspace","datastore","featuretype")
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

class TestFeatureTypeWMSService(Task):
    """
    Test the wms service of the feature type
    """
    arguments = ("workspace","datastore","featuretype","bbox","srs","dimension","format")
    category = "Test Feature WMS Service"

    bbox = None
    srs = None
    dimension = None
    format = None

    def __init__(self,workspace,datastore,featuretype,featuredetail,post_actions_factory = None,zoom=12):
        super().__init__(post_actions_factory = post_actions_factory) 
        self.workspace = workspace
        self.datastore = datastore
        self.featuretype = featuretype
        self.featuredetail = featuredetail
        self.zoom = zoom

    def get_tileposition(self,geoserver):
        #get the intersection between layer_box and settings.MAX_BBOX
        layer_bbox = [ self.featuredetail["latLonBoundingBox"][k] for k in ("minx","miny","maxx","maxy")]
    
        if layer_bbox[0] < settings.MAX_BBOX[0]:
            layer_bbox[0] = settings.MAX_BBOX[0]
        if layer_bbox[1] < settings.MAX_BBOX[1]:
            layer_bbox[1] = settings.MAX_BBOX[1]
    
        if layer_bbox[2] > settings.MAX_BBOX[2]:
            layer_bbox[2] = settings.MAX_BBOX[2]
    
        if layer_bbox[3] > settings.MAX_BBOX[3]:
            layer_bbox[3] = settings.MAX_BBOX[3]
        
        center_point = [(layer_bbox[0] + layer_bbox[2])/2,(layer_bbox[1] + layer_bbox[3])/2]
        xtile,ytile = geoserver.get_tileposition(center_point[0],center_point[1],self.zoom,gridset = settings.GWC_GRIDSET)
        return (xtile,ytile)
        
    def _format_result(self):
        return "image size = {}".format(self.result)

    def _exec(self,geoserver):
        xtile,ytile = self.get_tileposition(geoserver)
        self.bbox = geoserver.get_tilebbox(self.zoom,xtile,ytile,gridset = settings.GWC_GRIDSET)
        gridset_data = geoserver.get_gridset(settings.GWC_GRIDSET)
        self.srs = gridset_data["srs"]
        self.format = settings.MAP_FORMAT
        self.dimension = (gridset_data["tileWidth"],gridset_data["tileWidth"])


        img = geoserver.get_map(self.workspace,self.featuretype,self.bbox,
            srs=self.srs,
            width=self.dimension[0],
            height=self.dimension[1],
            format=self.format
        )
        try:
            return os.path.getsize(img)
        finally:
            os.remove(img)
            pass

def createtasks_ListFeatureTypes(listDatastoresTask,limit = 0):
    """
    a generator to return featuretypes tasks
    """
    if not listDatastoresTask.result:
        return
    row = 0
    for store in listDatastoresTask.result:
        row += 1
        if limit > 0 and row > limit:
            break
        yield ListFeatureTypes(listDatastoresTask.workspace,store,post_actions_factory=listDatastoresTask.post_actions_factory)


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


def createtasks_TestFeatureTypeWMSService(getFeatureTypeDetailTask,limit = 0):
    """
    a generator to return TestFeatureTypeWMSService tasks
    """
    if not getFeatureTypeDetailTask.result:
        return
    #get the intersection between layer_box and settings.MAX_BBOX
    layer_bbox = getFeatureTypeDetailTask.result.get("latLonBoundingBox")
    if not layer_bbox or any(layer_bbox.get(k) is None for k in ("minx","miny","maxx","maxy")):
        return

    yield TestFeatureTypeWMSService(
        getFeatureTypeDetailTask.workspace,
        getFeatureTypeDetailTask.datastore,
        getFeatureTypeDetailTask.featuretype,
        getFeatureTypeDetailTask.result,
        post_actions_factory=getFeatureTypeDetailTask.post_actions_factory)





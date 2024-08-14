import json
import logging
import os

from .base import Task
from .. import settings

logger = logging.getLogger(__name__)

class TestFeatureTypeWMSService(Task):
    """
    Test the wms service of the feature type
    """
    arguments = ("workspace","datastore","featuretype","style","bbox","srs","dimension","format")
    category = "Test Feature WMS Service"

    bbox = None
    srs = None
    dimension = None
    format = settings.MAP_FORMAT
    

    def __init__(self,workspace,datastore,featuretype,layer_bbox,style,post_actions_factory = None,zoom=12):
        super().__init__(post_actions_factory = post_actions_factory) 
        self.workspace = workspace
        self.datastore = datastore
        self.featuretype = featuretype
        self.layer_bbox = layer_bbox
        self.style = style
        self.zoom = zoom

    def get_tileposition(self,geoserver):
        #get the intersection between layer_box and settings.MAX_BBOX
        if self.layer_bbox[0] < settings.MAX_BBOX[0]:
            self.layer_bbox[0] = settings.MAX_BBOX[0]
        if self.layer_bbox[1] < settings.MAX_BBOX[1]:
            self.layer_bbox[1] = settings.MAX_BBOX[1]
    
        if self.layer_bbox[2] > settings.MAX_BBOX[2]:
            self.layer_bbox[2] = settings.MAX_BBOX[2]
    
        if self.layer_bbox[3] > settings.MAX_BBOX[3]:
            self.layer_bbox[3] = settings.MAX_BBOX[3]
        
        center_point = [(self.layer_bbox[0] + self.layer_bbox[2])/2,(self.layer_bbox[1] + self.layer_bbox[3])/2]
        xtile,ytile = geoserver.get_tileposition(center_point[0],center_point[1],self.zoom,gridset = settings.GWC_GRIDSET)
        return (xtile,ytile)
        
    def _format_result(self):
        return "image size = {}".format(self.result)

    def _exec(self,geoserver):
        xtile,ytile = self.get_tileposition(geoserver)
        self.bbox = geoserver.get_tilebbox(self.zoom,xtile,ytile,gridset = settings.GWC_GRIDSET)
        gridset_data = geoserver.get_gridset(settings.GWC_GRIDSET)
        self.srs = gridset_data["srs"]
        self.dimension = (gridset_data["tileWidth"],gridset_data["tileWidth"])


        img = geoserver.get_map(self.workspace,self.featuretype,self.bbox,
            srs=self.srs,
            style=self.style or "",
            width=self.dimension[0],
            height=self.dimension[1],
            format=self.format
        )
        try:
            return os.path.getsize(img)
        finally:
            try:
                os.remove(img)
            except:
                logger.error("Failed to delete temporary file '{}'".format(img))
                pass

class TestFeatureTypeWMTSService(TestFeatureTypeWMSService):
    """
    Test the wms service of the feature type
    """
    arguments = ("workspace","datastore","featuretype","gridset","zoom","row","column","format")
    category = "Test Feature WMTS Service"
    row = None
    column = None

    def __init__(self,workspace,datastore,featuretype,layer_bbox,style,post_actions_factory = None,zoom=12,gridset=settings.GWC_GRIDSET):
        super().__init__(workspace,datastore,featuretype,layer_bbox,style,post_actions_factory = post_actions_factory,zoom=zoom) 
        self.gridset = gridset

    def _exec(self,geoserver):
        self.column,self.row = self.get_tileposition(geoserver)
        self.format = settings.MAP_FORMAT

        img = geoserver.get_tile(self.workspace,self.featuretype,self.zoom,self.row,self.column,
            gridset=self.gridset,
            style=self.style or "",
            format=self.format
        )
        try:
            return os.path.getsize(img)
        finally:
            try:
                os.remove(img)
            except:
                logger.error("Failed to delete temporary file '{}'".format(img))
                pass

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

    layer_bbox = [layer_bbox[k] for k in ("minx","miny","maxx","maxy")]

    yield TestFeatureTypeWMSService(
        getFeatureTypeDetailTask.workspace,
        getFeatureTypeDetailTask.datastore,
        getFeatureTypeDetailTask.featuretype,
        layer_bbox,
        None,
        post_actions_factory=getFeatureTypeDetailTask.post_actions_factory)

    if getFeatureTypeDetailTask.result.get("alternativeStyles"):
        for style in getFeatureTypeDetailTask.result["alternativeStyles"]:
            yield TestFeatureTypeWMSService(
                getFeatureTypeDetailTask.workspace,
                getFeatureTypeDetailTask.datastore,
                getFeatureTypeDetailTask.featuretype,
                layer_bbox,
                getFeatureTypeDetailTask.result["defaultStyle"],
                post_actions_factory=getFeatureTypeDetailTask.post_actions_factory
            )


def createtasks_TestFeatureTypeWMTSService(getFeatureTypeDetailTask,limit = 0):
    """
    a generator to return TestFeatureTypeWMSService tasks
    """
    if not getFeatureTypeDetailTask.result:
        return
    if not getFeatureTypeDetailTask.result.get("gwc"):
        return
    #get the intersection between layer_box and settings.MAX_BBOX
    layer_bbox = getFeatureTypeDetailTask.result.get("latLonBoundingBox")
    if not layer_bbox or any(layer_bbox.get(k) is None for k in ("minx","miny","maxx","maxy")):
        return
    
    layer_bbox = [layer_bbox[k] for k in ("minx","miny","maxx","maxy")]

    for gridset in settings.GWC_GRIDSETS:
        if not any(gridsetdata["gridSetName"] == gridset  for gridsetdata in getFeatureTypeDetailTask.result["gwc"]["gridSubsets"]):
            continue
        yield TestFeatureTypeWMTSService(
            getFeatureTypeDetailTask.workspace,
            getFeatureTypeDetailTask.datastore,
            getFeatureTypeDetailTask.featuretype,
            layer_bbox,
            None,
            post_actions_factory=getFeatureTypeDetailTask.post_actions_factory,
            gridset=gridset
        )


import json
import logging
import os
from pyproj import Transformer

from .base import Task
from .. import settings
from .. import utils

logger = logging.getLogger(__name__)

class WMTSGetCapabilitiesTask(Task):
    category = "Get Capabilities"
    arguments = ("service",)
    keyarguments = ("service",)
    service = "WMTS"
    url = None

    def _format_result(self):
        return "URL : {}\r\nCapabilities File Size = {}".format(self.url or "",self.result)

    def _exec(self,geoserver):
        self.url = geoserver.wmtscapabilities_url()
        file = geoserver.get_wmtscapabilities()
        try:
            return os.path.getsize(file)
        finally:
            try:
                os.remove(file)
            except:
                logger.error("Failed to delete temporary file '{}'".format(file))
                pass

class TestWMTSService(Task):
    """
    Test the wms service of layer
    """
    row = None
    column = None
    format = settings.TEST_FORMAT
    url = None

    def __init__(self,workspace,store,layername,srs,layer_bbox,style,post_actions_factory = None,zoom=-1,gridset=settings.GWC_GRIDSET):
        """
        if zoom is -1. zoom will be set to the level which one tile contains the whole layerbox if zoom is -1, and then find the row and column
        otherwise, find the center point of layer box , and then find the row and column based on the zoom level
        """
        super().__init__(post_actions_factory = post_actions_factory) 
        self.gridset = gridset
        self.workspace = workspace
        self._store = store
        self._layername = layername
        self.srs = srs
        self.style = style
        self.zoom = zoom
        self.gridset = gridset
        self.gridsetdata = None
        self.layer_bbox = layer_bbox
        self.layer_bbox_gridset = None


    def _format_result(self):
        if self.layer_bbox_gridset is None or self.layer_bbox == self.layer_bbox_gridset:
            return """URL : {}
srs : {}
layer bbox : {}
image size : {}
""".format(self.url or "",self.srs,self.layer_bbox,self.result)
        else:
            return """URL : {}
srs : {}
layer bbox : {}
gridset srs : {}
layer bbox for gridset : {}
image size : {}
""".format(self.url or "",self.srs,self.layer_bbox,self.gridsetdata["srs"],self.layer_bbox_gridset,self.result)

    def set_with_gridset(self,geoserver):
        self.gridsetdata = geoserver.get_gridset(self.gridset)
        if self.srs.upper() != self.gridsetdata["srs"]:
            layer_bbox = list(self.layer_bbox)
            transformer = Transformer.from_crs(self.srs, self.gridsetdata["srs"])
            layer_bbox[1], layer_bbox[0] = transformer.transform(layer_bbox[0],layer_bbox[1])
            layer_bbox[3], layer_bbox[2] = transformer.transform(layer_bbox[2],layer_bbox[3])
        else:
            layer_bbox = self.layer_bbox

        for i in range(len(layer_bbox)):
            layer_bbox[i] = float(layer_bbox[i])

        """
        if self.gridsetdata["srs"].upper() in ("EPSG:4326","EPSG:4283"): 
            if layer_bbox[0] < settings.MAX_BBOX[0]:
                layer_bbox[0] = settings.MAX_BBOX[0]
            if layer_bbox[1] < settings.MAX_BBOX[1]:
                layer_bbox[1] = settings.MAX_BBOX[1]
    
            if layer_bbox[2] > settings.MAX_BBOX[2]:
                layer_bbox[2] = settings.MAX_BBOX[2]
    
            if layer_bbox[3] > settings.MAX_BBOX[3]:
                layer_bbox[3] = settings.MAX_BBOX[3]
        """

        #get the gwc details
        maxZoom = len(self.gridsetdata["resolutions"]) - 1
        if self.zoom >= 0:
            if self.zoom > maxZoom:
                self.zoom = maxZoom
        else:
            zoom = maxZoom
            while zoom > 0:
                if geoserver.get_tile_count(self.gridset,self.layer_bbox,zoom) > 1:
                    zoom -= 1
                else:
                    self.zoom = zoom
                    break

            if self.zoom < 0:
                self.zoom = 0

        self.layer_bbox_gridset = layer_bbox

    def get_tileposition(self,geoserver):
        #get the intersection between layer_box and settings.MAX_BBOX
        self.set_with_gridset(geoserver)
        
        center_point = [(self.layer_bbox_gridset[0] + self.layer_bbox_gridset[2])/2,(self.layer_bbox_gridset[1] + self.layer_bbox_gridset[3])/2]
        xtile,ytile = geoserver.get_tileposition(center_point[0],center_point[1],self.zoom,gridset = self.gridset)
        return (xtile,ytile)

    def _exec(self,geoserver):
        self.column,self.row = self.get_tileposition(geoserver)
        self.format = settings.TEST_FORMAT

        self.url = geoserver.tile_url(
            self.workspace,
            self._layername,
            self.zoom,
            self.row,
            self.column,
            gridset=self.gridset,
            style=self.style or "",
            format=self.format
        )
        img = geoserver.get_tile(
            self.workspace,
            self._layername,
            self.zoom,
            self.row,
            self.column,
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

class TestWMSService(TestWMTSService):
    """
    Test the wms service of the feature type
    """
    bbox = None
    srs = None
    dimension = None

    def __init__(self,workspace,store,layername,srs,layer_bbox,style,post_actions_factory = None,zoom=-1,gridset=settings.GWC_GRIDSET,detailTask=None):
        super(). __init__(workspace,store,layername,srs,layer_bbox,style,post_actions_factory = post_actions_factory,zoom=zoom,gridset=gridset)
        self.detailTask = detailTask

    def set_with_gridset(self,geoserver):
        super().set_with_gridset(geoserver)
        self.dimension = (self.gridsetdata["tileWidth"],self.gridsetdata["tileWidth"])

    def _exec(self,geoserver):
        xtile,ytile = self.get_tileposition(geoserver)
        self.bbox = geoserver.get_tilebbox(self.zoom,xtile,ytile,gridset = self.gridset)
        self.url = geoserver.map_url(
            self.workspace,
            self._layername,
            self.bbox,
            srs=self.gridsetdata["srs"],
            style=self.style or "",
            width=self.dimension[0],
            height=self.dimension[1],
            format=self.format
        )

        img = geoserver.get_map(
            self.workspace,
            self._layername,
            self.bbox,
            srs=self.gridsetdata["srs"],
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

class TestWMTSService4FeatureType(TestWMTSService):
    """
    Test the wms service of the feature type
    """
    arguments = ("workspace","datastore","featuretype","gridset","zoom","row","column","style","format")
    keyarguments = ("workspace","datastore","featuretype","gridset","style","format")
    category = "Test WMTS Service for FeatureType"

    @property
    def datastore(self):
        return self._store

    @property 
    def featuretype(self):
        return self._layername
    
class TestWMSService4FeatureType(TestWMSService,TestWMTSService4FeatureType):
    arguments = ("workspace","datastore","featuretype","srs","bbox","style","dimension","format")
    keyarguments = ("workspace","datastore","featuretype","srs","style","dimension","format")
    category = "Test WMS Service for FeatureType"

class TestWMTSService4Coverage(TestWMTSService):
    """
    Test the wms service of the coverage
    """
    arguments = ("workspace","coveragestore","coverage","gridset","zoom","row","column","style","format")
    keyarguments = ("workspace","coveragestore","coverage","gridset","style","format")
    category = "Test WMTS Service for Coverage"

    @property
    def coveragestore(self):
        return self._store

    @property 
    def coverage(self):
        return self._layername
    
class TestWMSService4Coverage(TestWMSService,TestWMTSService4Coverage):
    arguments = ("workspace","coveragestore","coverage","srs","bbox","style","dimension","format")
    keyarguments = ("workspace","coveragestore","coverage","srs","style","dimension","format")
    category = "Test WMS Service for Coverage"

class TestWMTSService4WMSLayer(TestWMTSService):
    category = "Test WMTS Service for WMSLayer"
    arguments = ("workspace","wmsstore","layername","gridset","zoom","row","column","style","format")
    keyarguments = ("workspace","wmsstore","layername","gridset","style","format")

    @property
    def wmsstore(self):
        return self._store
    
    @property 
    def layername(self):
        return self._layername
    
class TestWMSService4WMSLayer(TestWMSService,TestWMTSService4WMSLayer):
    arguments = ("workspace","wmsstore","layername","srs","bbox","style","dimension","format")
    keyarguments = ("workspace","wmsstore","layername","srs","style","dimension","format")
    category = "Test WMS Service for WMSLayer"


class TestWMTSService4Layergroup(TestWMTSService):
    category = "Test WMTS Service for Layergroup"
    arguments = ("workspace","layergroup","gridset","zoom","row","column","style")
    keyarguments = ("workspace","layergroup","gridset","style")

    def __init__(self,workspace,layergroup,layer_bbox,style,post_actions_factory = None,zoom=-1,gridset=settings.GWC_GRIDSET):
        super().__init__(workspace,None,layergroup,None,layer_bbox,style,post_actions_factory = post_actions_factory,zoom=zoom,gridset=gridset)

    @property 
    def layergroup(self):
        return self._layername
    
class TestWMSService4Layergroup(TestWMSService,TestWMTSService4Layergroup):
    arguments = ("workspace","layergroup","srs","bbox","style","dimension","format")
    keyarguments = ("workspace","layergroup","srs","style","dimension","format")
    category = "Test WMS Service for Layergroup"

    def __init__(self,workspace,layergroup,layer_bbox,style,post_actions_factory = None,zoom=-1,gridset=settings.GWC_GRIDSET,detailTask=None):
        super().__init__(workspace,None,layergroup,None,layer_bbox,style,post_actions_factory = post_actions_factory,zoom=zoom,gridset=gridset)
        self.detailTask = detailTask

def createtasks_TestWMSService4FeatureType(getFeatureTypeDetailTask,limit = 0):
    """
    a generator to return TestWMSService4FeatureType tasks
    """
    if not getFeatureTypeDetailTask.result:
        return
    if not getFeatureTypeDetailTask.enabled:
        return 
    if not getFeatureTypeDetailTask.result["geometry"]:
        return

    #get the intersection between layer_box and settings.MAX_BBOX
    layer_bbox = getFeatureTypeDetailTask.result.get("latLonBoundingBox")
    if not layer_bbox or any(layer_bbox.get(k) is None for k in ("minx","miny","maxx","maxy")):
        return

    layer_bbox = [layer_bbox[k] for k in ("minx","miny","maxx","maxy")]
    srs = getFeatureTypeDetailTask.result["latLonBoundingBox"]["crs"].upper()

    yield TestWMSService4FeatureType(
        getFeatureTypeDetailTask.workspace,
        getFeatureTypeDetailTask.datastore,
        getFeatureTypeDetailTask.featuretype,
        srs,
        layer_bbox,
        None,
        detailTask = getFeatureTypeDetailTask,
        zoom = settings.TEST_ZOOM,
        post_actions_factory=getFeatureTypeDetailTask.post_actions_factory)

    if getFeatureTypeDetailTask.result.get("alternativeStyles"):
        for style in getFeatureTypeDetailTask.result["alternativeStyles"]:
            yield TestWMSService4FeatureType(
                getFeatureTypeDetailTask.workspace,
                getFeatureTypeDetailTask.datastore,
                getFeatureTypeDetailTask.featuretype,
                srs,
                layer_bbox,
                style,
                detailTask = getFeatureTypeDetailTask,
                zoom = settings.TEST_ZOOM,
                post_actions_factory=getFeatureTypeDetailTask.post_actions_factory
            )


def createtasks_TestWMTSService4FeatureType(getFeatureTypeDetailTask,limit = 0):
    """
    a generator to return TestWMSService4FeatureType tasks
    """
    if not getFeatureTypeDetailTask.result:
        return
    if not getFeatureTypeDetailTask.gwcenabled:
        return 
    if not getFeatureTypeDetailTask.result["geometry"]:
        return
    #get the intersection between layer_box and settings.MAX_BBOX
    layer_bbox = getFeatureTypeDetailTask.result.get("latLonBoundingBox")
    if not layer_bbox or any(layer_bbox.get(k) is None for k in ("minx","miny","maxx","maxy")):
        return
    
    layer_bbox = [layer_bbox[k] for k in ("minx","miny","maxx","maxy")]
    srs = getFeatureTypeDetailTask.result["latLonBoundingBox"]["crs"].upper()

    for gridset in settings.GWC_GRIDSETS:
        gridsetdata = next((gridsetdata  for gridsetdata in getFeatureTypeDetailTask.result["gwc"]["gridSubsets"] if gridsetdata["gridSetName"] == gridset),None)
        if not gridsetdata:
            continue
        zoom = settings.TEST_ZOOM
        zoomStart = gridsetdata.get("zoomStart",0)
        zoomEnd = gridsetdata.get("zoomEnd",None)
        if zoom < zoomStart:
            zoom  = zoomStart
        if zoomEnd is not None and zoom > zoomEnd:
            zoom = zoomEnd

        yield TestWMTSService4FeatureType(
            getFeatureTypeDetailTask.workspace,
            getFeatureTypeDetailTask.datastore,
            getFeatureTypeDetailTask.featuretype,
            srs,
            layer_bbox,
            None,
            post_actions_factory=getFeatureTypeDetailTask.post_actions_factory,
            gridset=gridset,
            zoom=zoom
        )

def createtasks_TestWMSService4WMSLayer(getWMSLayerDetailTask,limit = 0):
    """
    a generator to return TestWMSService4FeatureType tasks
    """
    if not getWMSLayerDetailTask.result:
        return
    if not getWMSLayerDetailTask.enabled:
        return

    #get the intersection between layer_box and settings.MAX_BBOX
    layer_bbox = getWMSLayerDetailTask.result.get("latLonBoundingBox")
    if not layer_bbox or any(layer_bbox.get(k) is None for k in ("minx","miny","maxx","maxy")):
        return

    layer_bbox = [layer_bbox[k] for k in ("minx","miny","maxx","maxy")]
    srs = getWMSLayerDetailTask.result["latLonBoundingBox"]["crs"].upper()

    yield TestWMSService4WMSLayer(
        getWMSLayerDetailTask.workspace,
        getWMSLayerDetailTask.wmsstore,
        getWMSLayerDetailTask.layername,
        srs,
        layer_bbox,
        None,
        detailTask = getWMSLayerDetailTask,
        zoom = settings.TEST_ZOOM,
        post_actions_factory=getWMSLayerDetailTask.post_actions_factory)

def createtasks_TestWMSService4Coverage(getCoverageDetailTask,limit = 0):
    """
    a generator to return TestWMSService4Coverage tasks
    """
    if not getCoverageDetailTask.result:
        return
    if not getCoverageDetailTask.enabled:
        return

    #get the intersection between layer_box and settings.MAX_BBOX
    layer_bbox = getCoverageDetailTask.result.get("latLonBoundingBox")
    if not layer_bbox or any(layer_bbox.get(k) is None for k in ("minx","miny","maxx","maxy")):
        return

    layer_bbox = [layer_bbox[k] for k in ("minx","miny","maxx","maxy")]
    srs = getCoverageDetailTask.result["latLonBoundingBox"]["crs"].upper()
    yield TestWMSService4Coverage(
        getCoverageDetailTask.workspace,
        getCoverageDetailTask.coveragestore,
        getCoverageDetailTask.coverage,
        srs,
        layer_bbox,
        None,
        detailTask = getCoverageDetailTask,
        zoom = settings.TEST_ZOOM,
        post_actions_factory=getCoverageDetailTask.post_actions_factory)

def createtasks_TestWMTSService4WMSLayer(getWMSLayerDetailTask,limit = 0):
    """
    a generator to return TestWMSService4FeatureType tasks
    """
    if not getWMSLayerDetailTask.result:
        return
    if not getWMSLayerDetailTask.gwcenabled:
        return

    #get the intersection between layer_box and settings.MAX_BBOX
    layer_bbox = getWMSLayerDetailTask.result.get("latLonBoundingBox")
    if not layer_bbox or any(layer_bbox.get(k) is None for k in ("minx","miny","maxx","maxy")):
        return
    
    layer_bbox = [layer_bbox[k] for k in ("minx","miny","maxx","maxy")]
    srs = getWMSLayerDetailTask.result["latLonBoundingBox"]["crs"].upper()

    for gridset in settings.GWC_GRIDSETS:
        gridsetdata = next((gridsetdata  for gridsetdata in getWMSLayerDetailTask.result["gwc"]["gridSubsets"] if gridsetdata["gridSetName"] == gridset),None)
        if not gridsetdata:
            continue
        zoom = settings.TEST_ZOOM
        zoomStart = gridsetdata.get("zoomStart",0)
        zoomEnd = gridsetdata.get("zoomEnd",None)
        if zoom < zoomStart:
            zoom  = zoomStart
        if zoomEnd is not None and zoom > zoomEnd:
            zoom = zoomEnd

        yield TestWMTSService4WMSLayer(
            getWMSLayerDetailTask.workspace,
            getWMSLayerDetailTask.wmsstore,
            getWMSLayerDetailTask.layername,
            srs,
            layer_bbox,
            None,
            post_actions_factory=getWMSLayerDetailTask.post_actions_factory,
            gridset=gridset,
            zoom=zoom)

def createtasks_TestWMTSService4Coverage(getCoverageDetailTask,limit = 0):
    """
    a generator to return TestWMSService4FeatureType tasks
    """
    if not getCoverageDetailTask.result:
        return
    if not getCoverageDetailTask.gwcenabled:
        return

    #get the intersection between layer_box and settings.MAX_BBOX
    layer_bbox = getCoverageDetailTask.result.get("latLonBoundingBox")
    if not layer_bbox or any(layer_bbox.get(k) is None for k in ("minx","miny","maxx","maxy")):
        return
    
    layer_bbox = [layer_bbox[k] for k in ("minx","miny","maxx","maxy")]
    srs = getCoverageDetailTask.result["latLonBoundingBox"]["crs"].upper()

    for gridset in settings.GWC_GRIDSETS:
        gridsetdata = next((gridsetdata  for gridsetdata in getCoverageDetailTask.result["gwc"]["gridSubsets"] if gridsetdata["gridSetName"] == gridset),None)
        if not gridsetdata:
            continue
        zoom = settings.TEST_ZOOM
        zoomStart = gridsetdata.get("zoomStart",0)
        zoomEnd = gridsetdata.get("zoomEnd",None)
        if zoom < zoomStart:
            zoom  = zoomStart
        if zoomEnd is not None and zoom > zoomEnd:
            zoom = zoomEnd

        yield TestWMTSService4Coverage(
            getCoverageDetailTask.workspace,
            getCoverageDetailTask.coveragestore,
            getCoverageDetailTask.coverage,
            srs,
            layer_bbox,
            None,
            post_actions_factory=getCoverageDetailTask.post_actions_factory,
            gridset=gridset,
            zoom=zoom)

def createtasks_TestWMSService4Layergroup(getLayergroupDetailTask,limit = 0):
    """
    a generator to return TestWMSService4FeatureType tasks
    """
    if not getLayergroupDetailTask.result:
        return
    if not getLayergroupDetailTask.enabled:
        return

    #get the intersection between layer_box and settings.MAX_BBOX
    layer_bbox = getLayergroupDetailTask.result.get("bounds")
    if not layer_bbox or any(layer_bbox.get(k) is None for k in ("minx","miny","maxx","maxy")):
        return

    layer_bbox = [layer_bbox[k] for k in ("minx","miny","maxx","maxy")]
    srs = getLayergroupDetailTask.result.get("bounds")["crs"]

    yield TestWMSService4Layergroup(
        getLayergroupDetailTask.workspace,
        getLayergroupDetailTask.layergroup,
        layer_bbox,
        None,
        detailTask = getLayergroupDetailTask,
        zoom = settings.TEST_ZOOM,
        post_actions_factory=getLayergroupDetailTask.post_actions_factory)

def createtasks_TestWMTSService4Layergroup(getLayergroupDetailTask,limit = 0):
    """
    a generator to return TestWMSService4FeatureType tasks
    """
    if not getLayergroupDetailTask.result:
        return
    if not getLayergroupDetailTask.gwcenabled:
        return

    if not getLayergroupDetailTask.result.get("gwc") or not getLayergroupDetailTask.result["gwc"].get("enabled",False):
        return
    #get the intersection between layer_box and settings.MAX_BBOX
    layer_bbox = getLayergroupDetailTask.result.get("bounds")
    if not layer_bbox or any(layer_bbox.get(k) is None for k in ("minx","miny","maxx","maxy")):
        return
    
    layer_bbox = [layer_bbox[k] for k in ("minx","miny","maxx","maxy")]
    srs = getLayergroupDetailTask.result.get("bounds")["crs"]

    for gridset in settings.GWC_GRIDSETS:
        gridsetdata = next((gridsetdata  for gridsetdata in getLayergroupDetailTask.result["gwc"]["gridSubsets"] if gridsetdata["gridSetName"] == gridset),None)
        if not gridsetdata:
            continue
        zoom = settings.TEST_ZOOM
        zoomStart = gridsetdata.get("zoomStart",0)
        zoomEnd = gridsetdata.get("zoomEnd",None)
        if zoom < zoomStart:
            zoom  = zoomStart
        if zoomEnd is not None and zoom > zoomEnd:
            zoom = zoomEnd

        yield TestWMTSService4Layergroup(
            getLayergroupDetailTask.workspace,
            getLayergroupDetailTask.layergroup,
            layer_bbox,
            None,
            post_actions_factory=getLayergroupDetailTask.post_actions_factory,
            gridset=gridset,
            zoom=zoom
        )

def createtasks_TestWMTSServiceFromWMSService(testWMSService,limit = 0):
    """
    a generator to return TestWMSService4FeatureType tasks
    """
    if not testWMSService.detailTask.gwcenabled:
        return
    if testWMSService.is_failed:
        return

    for gridset in settings.GWC_GRIDSETS:
        gridsetdata = next((gridsetdata  for gridsetdata in testWMSService.detailTask.result["gwc"]["gridSubsets"] if gridsetdata["gridSetName"] == gridset),None)
        if not gridsetdata:
            continue
        zoom = settings.TEST_ZOOM
        zoomStart = gridsetdata.get("zoomStart",0)
        zoomEnd = gridsetdata.get("zoomEnd",None)
        if zoom < zoomStart:
            zoom  = zoomStart
        if zoomEnd is not None and zoom > zoomEnd:
            zoom = zoomEnd

        if isinstance(testWMSService,TestWMSService4FeatureType):
            #only test wmts for default style
            if not testWMSService.style:
                return
            yield TestWMTSService4FeatureType(
                testWMSService.workspace,
                testWMSService._store,
                testWMSService._layername,
                testWMSService.srs,
                testWMSService.layer_bbox,
                testWMSService.style,
                post_actions_factory=testWMSService.post_actions_factory,
                gridset=gridset,
                zoom=zoom)
        elif isinstance(testWMSService,TestWMSService4Coverage):
            yield TestWMTSService4Coverage(
                testWMSService.workspace,
                testWMSService._store,
                testWMSService._layername,
                testWMSService.srs,
                testWMSService.layer_bbox,
                testWMSService.style,
                post_actions_factory=testWMSService.post_actions_factory,
                gridset=gridset,
                zoom=zoom)
        elif isinstance(testWMSService,TestWMSService4WMSLayer):
            yield TestWMTSService4WMSLayer(
                testWMSService.workspace,
                testWMSService._store,
                testWMSService._layername,
                testWMSService.srs,
                testWMSService.layer_bbox,
                testWMSService.style,
                post_actions_factory=testWMSService.post_actions_factory,
                gridset=gridset,
                zoom=zoom)
        elif isinstance(testWMSService,TestWMSService4Layergroup):
            yield TestWMTSService4Layergroup(
                testWMSService.workspace,
                testWMSService._layername,
                testWMSService.layer_bbox,
                testWMSService.style,
                post_actions_factory=testWMSService.post_actions_factory,
                gridset=gridset,
                zoom=zoom)

def createtasks_WMTSGetCapabilities(task,limit = 0):
    """
    a generator to return WMTSSGetCapabilitiesTask
    """
    if not task.is_succeed:
        return
    yield WMTSGetCapabilitiesTask(
        post_actions_factory=task.post_actions_factory
    )
def createtasks_WMTSGetCapabilities(task,limit = 0):
    """
    a generator to return WMTSSGetCapabilitiesTask
    """
    if not task.is_succeed:
        return
    yield WMTSGetCapabilitiesTask(
        post_actions_factory=task.post_actions_factory
    )



import logging
import itertools
import collections
import requests
import math
import urllib.parse

from ..exceptions import *
from .. import settings

logger = logging.getLogger(__name__)

LAYER_DATA_TEMPLATE = """<?xml version="1.0" encoding="UTF-8"?>
    <GeoServerLayer>
        <name>{0}:{1}</name>
        <mimeFormats>
            {2}
        </mimeFormats>
        <enabled>true</enabled>
        <inMemoryCached>true</inMemoryCached>
        <gridSubsets>
            {3}
        </gridSubsets>
        <metaWidthHeight>
            <int>{4}</int>
            <int>{5}</int>
        </metaWidthHeight>
        <expireCache>{6}</expireCache>
        <expireClients>{7}</expireClients>
        <parameterFilters>
            <styleParameterFilter>
                <key>STYLES</key>
                <defaultValue></defaultValue>
            </styleParameterFilter>
        </parameterFilters>
        <gutter>{8}</gutter>
    </GeoServerLayer>
"""
EMPTY_LAYER_TEMPLATE="""<?xml version="1.0" encoding="UTF-8"?>
    <seedRequest>
        <name>{0}:{1}</name>
        <gridSetId>{2}</gridSetId>
        <zoomStart>0</zoomStart>
        <zoomStop>24</zoomStop>
        <type>truncate</type>
        <format>{3}</format>
        <threadCount>1</threadCount>
    </seedRequest>
"""
FORMAT_TEMPLATE = """<string>{}</string>"""
"""
0: name
1: extent
2: zoomstart
3: zoomstop
"""
GRIDSUBSET_TEMPLATE = """<gridSubset><gridSetName>{0}</gridSetName>{1}{2}{3}</gridSubset>"""
"""
0: minx, west longitude
1: miny, south latitude
2: maxx, east longitude
3: maxy, north logitude
"""
EXTENT_TEMPLATE = """
    <extent>
        <coords>
            <double>{0}</double>
            <double>{1}</double>
            <double>{2}</double>
            <double>{3}</double>
        </coords>
    </extent>"""
ZOOMSTART_TEMPLATE = """
    <zoomStart>{}</zoomStart>"""
ZOOMSTOP_TEMPLATE = """
    <zoomStop>{}</zoomStop>"""

INSTANCES = {}
class GridsetUtil(object):
    SRS = None
    INSTANCES = {}

    def __init__(self):
        INSTANCES[self.SRS] = self


    @staticmethod
    def get_instance(srs):
        try:
            return INSTANCES[srs.upper()]
        except:
            raise Exception("The SRS({}) Not Support".format(srs))

    def tile_bbox(self,zoom,x,y):
        """
        Return the bbox of a title
        """
        raise Exception("Not Implemented")
        

    def get_tile(self,x,y,zoom):
        """
        Retun the tile position(x,y)
        """
        raise Exception("Not Implemented")

    def tiles(self,bbox, zoom):
        """
        Return a iterate to go through all the tiles included in the bbox
        """
        raise Exception("Not Implemented")

    def get_tile_count(self,bbox, zoom):
        """
        Return the total number of tiles in the bbox
        """
        raise Exception("Not Implemented")


class EPSG4326Util(GridsetUtil):
    SRS = "EPSG:4326"
    R2D = 180 / math.pi
    D2R = math.pi / 180

    def tile_lon(self,zoom,x) :
        assert x <= math.pow(2,zoom + 1),"The tile row index should be not greater than {}".format(int(math.pow(2,zoom + 1)))
        return x / math.pow(2.0, zoom + 1) * 360.0 - 180

    def tile_lat(self,zoom,y) :
        assert y <=math.pow(2,zoom),"The tile column index should be not greater than {}".format(int(math.pow(2,zoom)))
        return math.degrees(
            math.atan(math.sinh(math.pi - (2.0 * math.pi * y) / math.pow(2.0, zoom)))
        )
        #n = math.pi - 2 * math.pi * y / math.pow(2, zoom)
        #return self.R2D * math.atan(0.5 * (math.exp(n) - math.exp(-n)))

    def tile_bbox(self,zoom,x,y):
        """
        Return the bounding box(left bottom, right top)(minx,miny,maxx,maxy)
        """
        return (self.tile_lon(zoom,x),self.tile_lat(zoom,y + 1),self.tile_lon(zoom,x + 1),self.tile_lat(zoom,y))

    def get_tile(self,lon_deg, lat_deg, zoom):
        xtile = int((lon_deg + 180.0) / 360.0 * math.pow(2.0,zoom + 1))
        lat_rad = math.radians(lat_deg)
        ytile = int((1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * math.pow(2.0,zoom))
        return (xtile, ytile)

    def tiles(self,bbox, zoom):
        """
        bbox: left bottom, right top
        """
        left_bottom_tile = self.get_tile(bbox[0],bbox[1], zoom)
        right_top_tile = self.get_tile(bbox[2],bbox[3], zoom)

        for x in range(left_bottom_tile[0],right_top_tile[0] + 1):
            for y in range(right_top_tile[1],left_bottom_tile[1] + 1):
                yield (x,y)

    def get_tile_count(self,bbox, zoom):
        totalTileCount = 0
        left_bottom_tile = self.get_tile(bbox[0],bbox[1], zoom)
        right_top_tile = self.get_tile(bbox[2],bbox[3], zoom)

        return (right_top_tile[0] - left_bottom_tile[0] + 1) * (left_bottom_tile[1] - right_top_tile[1] + 1)

    def _decimal2minutes(decimal_degrees, is_latitude) :
        absdegrees = Math.abs(decimal_degrees);
        degrees = math.floor(absdegrees)
        absdegrees -= degrees
        absdegrees *= 60
        minutes = math.floor(absdegrees);
        absdegrees -= minutes;
        absdegrees *= 60
        seconds = Math.floor(absdegrees);
        suffix = "";
        if is_latitude:
            suffix = "N" if decimal_degrees >= 0 else "S"
        else:
            suffix = "E" if decimal_degrees >= 0 else "W"
  
        return "{}\u00B0 {}' {}\" {}".format(degrees,minutes,seconds,suffix);

class EPSG4283Util(EPSG4326Util):
    SRS = "EPSG:4283"

def init_gridsetutil(util_class=GridsetUtil):
    for cls in util_class.__subclasses__():
        if cls.SRS:
            cls()
        init_gridsetutil(cls)

init_gridsetutil()

class GWCMixin(object):
    def gwclayers_url(self):
        return "{0}/gwc/rest/layers".format(self.geoserver_url)
    
    def gwclayer_url(self,workspace,layername,f=None):
        return "{0}/gwc/rest/layers/{1}:{2}{3}".format(self.geoserver_url,workspace,layername,".{}".format(f) if f else "")
    
    def gwclayer_seed_url(self,workspace,layername):
        return "{0}/gwc/rest/seed/{1}:{2}.xml".format(self.geoserver_url,workspace,layername)

    def wmtsservice_url(self,workspace,layername):
        return "{0}/gwc/service/wmts".format(self.geoserver_url,layername)

    def gridset_url(self,gridset):
        return "{}/gwc/rest/gridsets/{}".format(self.geoserver_url,gridset)

    def _handle_gwcresponse_error(self,res):
        if res.status_code >= 400 and res.text.startswith("Unknown layer:"):
            raise ResourceNotFound(response=res)
        super()._handle_response_error(res)

    _gridsets = {}
    def get_gridset(self,gridset):
        try:
            return self._gridsets[gridset]
        except KeyError as ex:
            res = self.get(self.gridset_url(gridset),headers=self.accept_header("json"))
            data = res.json()["gridSet"]
            data["srs"] = "EPSG:{}".format(data["srs"]["number"])
            self._gridsets[gridset] = data
            return data

    def list_gwclayers(self,workspace=None):
        """
        Return the gwc layers in workspace, if workspace is not None; otherwise return all gwc layers in all workspaces.
        Return the list of layers:[(workspace,layername)]
        """
        res = self.get(self.gwclayers_url(),headers=self.accept_header("json"))
        if workspace:
            prefix = "{}:".format(workspace)
            return [ name.split(":",1) if ":" in name else [None,name]  for name in res.json() if name.startswith(prefix)]
        else:
            return [ name.split(":",1) if ":" in name else [None,name]  for name in res.json()]
        
    def has_gwclayer(self,workspace,layername):
        return self.has(self.gwclayer_url(workspace,layername) , headers=self.accept_header("json"),error_handler=self._handle_gwcresponse_error)
            
    def get_gwclayer(self,workspace,layername):
        """
        Return a json object if exists; otherwise return None
        """
        try:
            res = self.get(self.gwclayer_url(workspace,layername) , headers=self.accept_header("json"),error_handler=self._handle_gwcresponse_error)
            return res.json().get("GeoServerLayer")
        except ResourceNotFound as ex:
            return None
            
    def delete_gwclayer(self,workspace,layername):
        if self.has_gwclayer(workspace,layername):
            res = self.delete(self.gwclayer_url(workspace,layername,f="xml"),error_handler=self._handle_gwcresponse_error)
            logger.debug("Succeed to delete the gwc layer({}:{})".format(workspace,layername))
        else:
            logger.debug("The gwc layer({}:{}) doesn't exist".format(workspace,layername))

    def update_gwclayer(self,workspace,layername,parameters,is_featuretype=None):
        """
        mimeFormats: format list
        gridSubsets: list of gridSubset.each gridSubset supports the following parameters
            name: the name of the gridSubset
            extent: the extent([minx,miny,maxx,maxy]) of the gridSubset. optional. 
            zoomstart: optional
            zoomstop: optional
        metaWidth: optional, default 1
        metaHeight: optional, default 1
        expireCache: optional, default 0
        expireClients: optional, default 0
        gutter: optional, default 100
        """
        if is_featuretype is None:
            is_featuretype = self.has_featuretype(workspace,layername)
        formats = parameters.get("mimeFormats",["image/png","image/jpeg"])
        if is_featuretype:
            formats = itertools.chain(formats,["application/json;type=geojson","application/json;type=topojson","application/x-protobuf;type=mapbox-vector","application/json;type=utfgrid"])
        layer_data = LAYER_DATA_TEMPLATE.format(
            workspace,
            layername,
            "".join(FORMAT_TEMPLATE.format(f) for f in formats),
            "".join(GRIDSUBSET_TEMPLATE.format(
                f["name"],
                EXTENT_TEMPLATE.format(*f.get("extent")) if f.get("extent") else "",
                ZOOMSTART_TEMPLATE.format(f.get("zoomStart")) if f.get("zoomStart") is not None else "",
                ZOOMSTOP_TEMPLATE.format(f.get("zoomStop")) if f.get("zoomStop") is not None else ""
            ) for f in parameters.get("gridSubsets",[{"name":"gda94"},{"name":"mercator"}])),
            parameters.get("metaWidth",1),
            parameters.get("metaHeight",1),
            parameters.get("expireCache",0),
            parameters.get("expireClients",0),
            parameters.get("gutter",100)
        )
    
        res = self.put(self.gwclayer_url(workspace,layername,f="xml"), headers=self.contenttype_header("xml"), data=layer_data,error_handler=self._handle_gwcresponse_error)
        logger.debug("Succeed to update the gwc layer({}:{}). ".format(workspace,layername))
    
    def empty_gwclayer(self,workspace,layername,gridsubsets=["gda94","mercator"],formats=["image/png","image/jpeg"]):
        for gridset in gridsubsets:
            for f in formats:
                layer_data = EMPTY_LAYER_TEMPLATE.format(
                    workspace,
                    layername,
                    gridset,
                    f
                )
                res = self.post(self.gwclayer_seed_url(workspace,layername),headers=collections.ChainMap(self.accept_header("json"),self.contenttype_header("xml")), data=layer_data,error_handler=self._handle_gwcresponse_error)
    
        #check whether the task is finished or not.
        finished = False
        while(finished):
            finished = True
            res = self.get(self.gwclayer_url(workspace,layername), headers=self.accept_header("json"),error_handler=self._handle_gwcresponse_error)
            tasks = res.json().get("long-array-array",[])
            for t in tasks:
                if t[3] == -1:
                    #aborted
                    raise Exception("Failed to empty the cache of the gwc layer({}:{}). some tasks are aborted".format(workspace,layername))
                elif t[3] in (0,1):
                    finished = False
                    break
            if not finished:
                time.sleep(1)

    def get_tileposition(self,x,y,zoom,gridset=settings.GWC_GRIDSET):
        return GridsetUtil.get_instance(self.get_gridset(gridset)["srs"]).get_tile(x,y,zoom)

    def get_tilebbox(self,zoom,xtile,ytile,gridset=settings.GWC_GRIDSET):
        return GridsetUtil.get_instance(self.get_gridset(gridset)["srs"]).tile_bbox(zoom,xtile,ytile)

    def get_tile(self,workspace,layername,zoom,row,column,gridset=settings.GWC_GRIDSET,format=settings.MAP_FORMAT,style=None,version=settings.WMTS_VERSION):
        params = "layer={0}:{1}&style={7}&tilematrixset={2}&Service=WMTS&Request=GetTile&Version={8}&Format={6}&TileMatrix={2}:{3}&TileCol={5}&TileRow={4}".format(workspace,layername,gridset,zoom,row,column,format,style or "",version)
        url = "{0}?{1}".format(self.wmtsservice_url(workspace,layername),urllib.parse.quote(params))
        res = self.get(url,headers=self.accept_header("jpeg"),error_handler=self._handle_gwcresponse_error)
        return res
        


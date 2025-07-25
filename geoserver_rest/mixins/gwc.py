import logging
import itertools
import collections
import requests
import math
import urllib.parse
import tempfile

from ..exceptions import *
from .. import settings

logger = logging.getLogger(__name__)
    
TRUNCATE_TEMPLATE = """<?xml version="1.0" encoding="UTF-8"?>
<truncateLayer>
    <layerName>{}:{}</layerName>
</truncateLayer>
"""

LAYER_DATA_TEMPLATE = """<?xml version="1.0" encoding="UTF-8"?>
    <GeoServerLayer>
        <name>{0}:{1}</name>
        <mimeFormats>{2}</mimeFormats>
        <gridSubsets>{3}</gridSubsets>
        <metaWidthHeight><int>{4}</int><int>{5}</int></metaWidthHeight>
        <expireCache>{6}</expireCache>
        <expireClients>{7}</expireClients>
        <gutter>{8}</gutter>
        <enabled>{9}</enabled>
        <inMemoryCached>true</inMemoryCached>
        <parameterFilters>
            <styleParameterFilter>
                <key>STYLES</key>
                <defaultValue></defaultValue>
            </styleParameterFilter>
        </parameterFilters>
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

    def get_bbox_tile(self,bbox):
        """
        bbox : [minx,miny,maxx,maxy]
        Return (zoom,x,y)
        """
        zoom = 5
        isbiggerbefore = None
        while(True):
            x,y= self.get_tile(bbox[0],bbox[1],zoom)
            tilebbox = self.tile_bbox(zoom,x,y)
            if tilebbox[0] <= bbox[0] and tilebbox[1] <= bbox[1] and tilebbox[2] >= bbox[2] and tilebbox[3] >= bbox[3]:
                #tilebbox bigger than the bbox.zoom in
                if isbiggerbefore == False:
                    return (zoom,x,y)
                else:
                    isbiggerbefore = True
                    zoom += 1
            else:
                #tilebbox smaller than the bbox.zoom out
                if isbiggerbefore == True:
                    return (zoom - 1,*self.get_tile(bbox[0],bbox[1],zoom - 1))
                else:
                    isbiggerbefore = False
                    zoom -= 1


class EPSG4326Util(GridsetUtil):
    SRS = "EPSG:4326"
    #R2D = 180 / math.pi
    #D2R = math.pi / 180

    def tile_lon(self,zoom,x) :
        assert x <= math.pow(2,zoom + 1),"The tile row index should be not greater than {}".format(int(math.pow(2,zoom + 1)))
        return x / math.pow(2.0, zoom + 1) * 360.0 - 180

    def tile_lat(self,zoom,y) :
        assert y <=math.pow(2,zoom),"The tile column index should be not greater than {}".format(int(math.pow(2,zoom)))
        #return math.degrees(
        #    math.atan(math.sinh(math.pi - (2.0 * math.pi * y) / math.pow(2.0, zoom)))
        #)
        #n = math.pi - 2 * math.pi * y / math.pow(2, zoom)
        #return self.R2D * math.atan(0.5 * (math.exp(n) - math.exp(-n)))
        return y / math.pow(2.0, zoom ) * -180.0 + 90

    def tile_bbox(self,zoom,x,y):
        """
        Return the bounding box(left bottom, right top)(minx,miny,maxx,maxy)
        """
        return (self.tile_lon(zoom,x),self.tile_lat(zoom,y + 1),self.tile_lon(zoom,x + 1),self.tile_lat(zoom,y))

    def get_tile(self,lon_deg, lat_deg, zoom):
        xtile = int((lon_deg + 180.0) / 360.0 * math.pow(2.0,zoom + 1))
        #lat_rad = math.radians(lat_deg)
        #ytile = int((1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * math.pow(2.0,zoom))
        ytile = int((lat_deg - 90.0) / -180.0 * math.pow(2.0,zoom))
        return (xtile, ytile)


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

    def gwclayer_truncate_url(self,workspace,layername,requestType="truncateLayer"):
        #return "{0}/gwc/rest/masstruncate?requestType={2}&layer={1}".format(self.geoserver_url,self.urlencode("{}:{}".format(workspace,layername)),requestType)
        return "{0}/gwc/rest/masstruncate".format(self.geoserver_url)

    def wmtsservice_url(self,workspace,layername):
        return "{0}/gwc/service/wmts".format(self.geoserver_url,layername)

    def gridset_url(self,gridset):
        return "{}/gwc/rest/gridsets/{}".format(self.geoserver_url,gridset)

    def tile_url(self,workspace,layername,zoom,row,column,gridset=settings.GWC_GRIDSET,format="image/jpeg",style=None,version=settings.WMTS_VERSION):
        parameters = "service=WMTS&version={0}&request=GetTile&layer={1}%3A{2}&style={7}&format={8}&tilematrixset={3}&TileMatrix={3}%3A{4}&TileCol={6}&TileRow={5}".format(
            version,workspace,layername,gridset,zoom,row,column,style or "",urllib.parse.quote(format)
        )
        return "{0}/gwc/service/wmts?{1}".format(self.geoserver_url,parameters)

    def wmtscapabilities_url(self,version="1.1.1"):
        return "{}/gwc/service/wmts?service=WMTS&version=1.1.1&request=GetCapabilities".format(self.geoserver_url)

    def get_wmtscapabilities(self,version="1.1.1",outputfile=None):
        res = self.get(self.wmtscapabilities_url(version=version),headers=self.accept_header("xml"),timeout=settings.GETCAPABILITY_TIMEOUT)
        if outputfile:
            output = open(outputfile,'wb')
        else:
            output = tempfile.NamedTemporaryFile(
                mode='wb',
                prefix="gswmtscapabilities_",
                suffix=".xml",
                delete = False, 
                delete_on_close = False
            )
            outputfile = output.name
        try:
            for data in res.iter_content(chunk_size = 1024):
                output.write(data)
            logger.debug("WMTS capabilities was saved to {}".format(outputfile))
            return outputfile
        finally:
            output.close()
    

    def _handle_gwcresponse_error(self,res):
        if res.status_code >= 400 and "Unknown layer" in res.text:
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
        res = self.get(self.gwclayer_url(workspace,layername) , headers=self.accept_header("json"),error_handler=self._handle_gwcresponse_error)
        return res.json().get("GeoServerLayer")
            
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
        enabled: optional, default True
        """
        if is_featuretype is None:
            is_featuretype = self.has_featuretype(workspace,layername)
        formats = parameters.get("mimeFormats",["image/png","image/jpeg"])
        if is_featuretype:
            formats = itertools.chain(formats,["application/json;type=geojson","application/json;type=topojson","application/x-protobuf;type=mapbox-vector","application/json;type=utfgrid"])

        gridSubsets = parameters.get("gridSubsets",[{"name":"gda94"},{"name":"mercator"}])
        metaWidth = parameters.get("metaWidth",1)
        metaHeight = parameters.get("metaHeight",1)
        expireCache = parameters.get("expireCache",0)
        expireClients = parameters.get("expireClients",0)
        gutter = parameters.get("gutter",100)
        enabled = parameters.get("enabled",True)

        layer_data = LAYER_DATA_TEMPLATE.format(
            workspace,
            layername,
            "".join(FORMAT_TEMPLATE.format(f) for f in formats),
            "".join(GRIDSUBSET_TEMPLATE.format(
                f["name"],
                EXTENT_TEMPLATE.format(*f.get("extent")) if f.get("extent") else "",
                ZOOMSTART_TEMPLATE.format(f.get("zoomStart")) if f.get("zoomStart") is not None else "",
                ZOOMSTOP_TEMPLATE.format(f.get("zoomStop")) if f.get("zoomStop") is not None else ""
            ) for f in gridSubsets),
            metaWidth, 
            metaHeight,
            expireCache,
            expireClients,
            gutter,
            "true" if enabled else "false"
        )
        res = self.put(self.gwclayer_url(workspace,layername,f="xml"), headers=self.contenttype_header("xml"), data=layer_data,error_handler=self._handle_gwcresponse_error)
        logger.debug("Succeed to update the gwc layer({}:{}). ".format(workspace,layername))
    
    def empty_gwclayer(self,workspace,layername):
        self.post(
            self.gwclayer_truncate_url(workspace,layername),
            TRUNCATE_TEMPLATE.format(workspace,layername),
            headers={'content-type': 'text/xml'})

    def get_tileposition(self,x,y,zoom,gridset=settings.GWC_GRIDSET):
        return GridsetUtil.get_instance(self.get_gridset(gridset)["srs"]).get_tile(x,y,zoom)

    def get_tilebbox(self,zoom,xtile,ytile,gridset=settings.GWC_GRIDSET):
        return GridsetUtil.get_instance(self.get_gridset(gridset)["srs"]).tile_bbox(zoom,xtile,ytile)

    def get_tile_count(self,gridset,bbox,zoom):
        return GridsetUtil.get_instance(self.get_gridset(gridset)["srs"]).get_tile_count(bbox,zoom)

    def get_tile(self,workspace,layername,zoom=None,row=None,column=None,gridset=settings.GWC_GRIDSET,format="image/jpeg",style=None,version=settings.WMTS_VERSION,outputfile=None):
        """
        if zoom,row or column is None, will return a tile conver the whole layer
        outputfile: a temporary file will be created if outputfile is None, the client has the responsibility to delete the outputfile
        If succeed, save the image to outputfile
        """
        if zoom is None or row is None or column is None:
            try:
                layerdata = self.get_featuretype(workspace,layername)
                bbox = self.get_featuretype_field(layerdata,"latLonBoundingBox")
                bbox = (bbox["minx"],bbox["miny"],bbox["maxx"],bbox["maxy"])
            except ResourceNotFound as ex:
                try:
                    layerdata = self.get_wmslayer(workspace,layername)
                    bbox = self.get_wmslayer_field(layerdata,"latLonBoundingBox")
                    bbox = (bbox["minx"],bbox["miny"],bbox["maxx"],bbox["maxy"])
                except ResourceNotFound as ex:
                    layerdata = self.get_layergroup(workspace,layername)
                    bbox = self.get_layergroup_field(layerdata,"bounds")
                    bbox = (bbox["minx"],bbox["miny"],bbox["maxx"],bbox["maxy"])
            zoom,column,row = GridsetUtil.get_instance(self.get_gridset(gridset)["srs"]).get_bbox_tile(bbox)

        url = self.tile_url(workspace,layername,zoom,row,column,gridset=gridset,format=format,style=style,version=version)
        logger.debug("Tile url={}".format(url))
        res = self.get(url,headers=self.accept_header("jpeg"),error_handler=self._handle_gwcresponse_error,timeout=settings.WMTS_TIMEOUT)
        if res.headers.get("content-type") != format:
            if any( t in res.headers.get("content-type","") for t in ("text/","xml","css","json","javascript")):
                try:
                    msg = res.text
                except:
                    raise GetMapFailed("Failed to get the map of layer({}:{}).Expect '{}', but got '{}'".format(workspace,layername,format,res.headers.get("content-type","")))

                raise GetMapFailed("Failed to get the map of layer({}:{}).{}".format(workspace,layername,msg))
            else:
                raise GetMapFailed("Failed to get the map of layer({}:{}).Expect '{}', but got '{}'".format(workspace,layername,format,res.headers.get("content-type","")))
        if outputfile:
            output = open(outputfile,'wb')
        else:
            output = tempfile.NamedTemporaryFile(
                mode='wb',
                prefix="gswmts_",
                suffix=".{}".format(format.rsplit("/",1)[1] if "/" in format else format),
                delete = False, 
                delete_on_close = False
            )
            outputfile = output.name
        try:
            for data in res.iter_content(chunk_size = 1024):
                output.write(data)
            logger.debug("WMTS image was saved to {}".format(outputfile))
            return outputfile
        finally:
            output.close()

    def get_gwclayer_field(self,layerdata,field):
        """
        field:
            name
            id
            enabled:
            gridSubsets: list of dict({name,extent(optional),zoomStart(optional),zoomStop(optional)})
            expireCache
            expireClients
            gutter
            metaWidthHeight

        <gridSubsets>
            {3}
        </gridSubsets>
        """
        if field == "gridSubsets":
            gridsets = []
            for data in layerdata.get("gridSubsets",[]):
                gridset = {
                    "name":data["gridSetName"]
                }
                if "extent" in data:
                    gridset["extent"] = data["extent"]["coords"]
                if "zoomStart" in data:
                    gridset["zoomStart"] = data["zoomStart"]
                if "zoomStop" in data:
                    gridset["zoomStop]"] = data["zoomStop"]
                gridsets.append(gridset)
            return gridsets
        elif field == "metaWidth":
            return layerdata.get("metaWidthHeight",[None,None])[0]
        elif field == "metaHeight":
            return layerdata.get("metaWidthHeight",[None,None])[1]
        else:
            return layerdata.get(field)

    

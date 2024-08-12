import logging
import os

logger = logging.getLogger(__name__)

LAYER_TEMPLATE = """<?xml version="1.0" encoding="UTF-8"?>
<wmsLayer>
    <name>{2}</name>
    <nativeName>{6}</nativeName>
    <namespace>
        <name>{0}</name>
    </namespace>
    <title>{3}</title>
    <abstract>{4}</abstract>
    <description>{5}</description>
    <keywords>
        {7}
    </keywords>
    {8}
    {9}
    {10}
    {11}
    <projectionPolicy>FORCE_DECLARED</projectionPolicy>
    <enabled>true</enabled>
    <store class="wmsStore">
        <name>{0}:{1}</name>
    </store>
</wmsLayer>
"""
"""
minx: west longitude
miny: south latitude
maxx: east longitude
maxy: north logitude
"""
NATIVEBOUNDINGBOX_TEMPLATE="""
    <nativeBoundingBox>
        <minx>{0}</minx>
        <maxx>{2}</maxx>
        <miny>{1}</miny>
        <maxy>{3}</maxy>
        <crs>{4}</crs>
    </nativeBoundingBox>
"""
"""
minx: west longitude
miny: south latitude
maxx: east longitude
maxy: north logitude
"""
LATLONBOUNDINGBOX_TEMPLATE="""
    <latLonBoundingBox>
        <minx>{0}</minx>
        <maxx>{2}</maxx>
        <miny>{1}</miny>
        <maxy>{3}</maxy>
        <crs>{4}</crs>
    </latLonBoundingBox>
"""
NATIVECRS_TEMPALTE = """<nativeCRS>{}</nativeCRS>"""
SRS_TEMPLATE = """<srs>{}</srs>"""
KEYWORD_TEMPLATE = """<string>{}</string>"""

class WMSLayerMixin(object):
    def wmslayers_url(self,workspace,storename=None):
        if storename:
            return "{0}/rest/workspaces/{1}/wmsstores/{2}/wmslayers".format(self.geoserver_url,workspace,storename)
        else:
            return "{0}/rest/workspaces/{1}/wmslayers".format(self.geoserver_url,workspace)
    
    def wmslayer_url(self,workspace,layername,storename="",format="json"):
        if storename:
            return "{0}/rest/workspaces/{1}/wmsstores/{2}/wmslayers/{3}.{4}".format(self.geoserver_url,workspace,storename,layername,format)
        else:
            return "{0}/rest/workspaces/{1}/wmslayers/{2}.{3}".format(self.geoserver_url,workspace,layername,format)
    
    def has_wmslayer(self,workspace,layername,storename=None):
        return self.has(self.wmslayer_url(workspace,layername,storename=storename,format="json"),headers=GeoserverUtils.accept_header("json"))
    
    def get_wmslayer(self,workspace,layername,storename=None):
        r = self.get(self.wmslayer_url(workspace,layername,storename=storename,format="json"),headers=self.accept_header("json"))
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json()["wmsLayer"]
    
    def list_wmslayers(self,workspace,storename):
        """
        Return the list of layers in the store if storename is not null;otherwise return all layers in the workspace
        """
        r = self.get(self.wmslayers_url(workspace,storename),headers=self.accept_header("json"))
        if r.status_code >= 300:
            raise Exception("Failed to list the wmslayers in wmsstore({}:{}). code = {},message = {}".format(workspace,storename,r.status_code, r.content))
    
        return [str(l["name"]) for l in (r.json().get("wmsLayers") or {}).get("wmsLayer") or [] ]
    
    def delete_wmslayer(self,workspace,layername):
        """
        Return True if deleted;otherwise return False if doesn't exist before
        """

        if not self.has_wmslayer(workspace,layername):
            logger.debug("The wmslayer({}:{}) doesn't exist".format(workspace,layername))
            if self.has_gwclayer(workspace,layername):
                self.delete_gwclayer(workspace,layername)
            return False
    
        r = self.delete(self.wmslayer_url(workspace,layername,format="xml"))
        if r.status_code >= 300:
            raise Exception("Failed to delete the wmslayer({}:{}). code = {} , message = {}".format(workspace,layername,r.status_code, r.content))
    
        logger.debug("Succeed to delete the wmslayer({}:{}).".format(workspace,layername))
        return True
    
    def update_wmslayer(self,workspace,storename,layername,parameters,create=None):
        """
        bbox format: minx, west longitude, miny, south latitude, maxx, east longitude,maxy, north logitude
        parameters:
            nativeName: the layer name in the upstream, if empty, use layername as nativeName
            nativeBoundingBox: a native bounding box (minx Longitude , miny Latitude , maxx Longitude , maxy Latitude,crs ); if doesn't exist, looking for bbox.
            latLonBoundingBox: a native bounding box (min Longitude , min Latitude , max Longitude , max Latitude,crs ); if doesn't exist, looking for bbox.
            bbox: a bounding box can be used by nativeBoundingBox and latLonBoundingBox if they doesn't exist
        Return True if created;otherwise return False if updated
        """
        if create is None:
            create = False if self.has_wmslayer(workspace,layername) else True
        if not create:
            if self.has_wmslayer(workspace,layername,storename=storename):
                #layer exists and in the same wmsstore
                create = False
            else:
                #layer exists,but in different wmsstore
                #delete the wmslayer and recreate it
                self.delete_wmslayer(workspace,layername)
                create = True

        nativeBoundingBox = parameters.get("nativeBoundingBox",parameters.get("bbox"))
        if nativeBoundingBox:
            nativeBoundingBox_xml = NATIVEBOUNDINGBOX_TEMPLATE.format(*nativeBoundingBox)
        else:
            nativeBoundingBox_xml = ""

        latLonBoundingBox = parameters.get("latLonBoundingBox",parameters.get("bbox"))
        if latLonBoundingBox:
            latLonBoundingBox_xml = LATLONBOUNDINGBOX_TEMPLATE.format(*latLonBoundingBox)
        else:
            latLonBoundingBox_xml = ""

        layer_data = LAYER_TEMPLATE.format(
            workspace,
            storename,
            layername,
            self.encode_xmltext(parameters.get("title") or layername),
            self.encode_xmltext(parameters.get("abstract")),
            self.encode_xmltext(parameters.get("description")),
            parameters.get("nativeName") or layername,
            os.linesep.join(KEYWORD_TEMPLATE.format(k) for k in  parameters.get('keywords', [])), 
            NATIVECRS_TEMPALTE.format(parameters.get("nativeCRS")) if parameters.get("nativeCRS") else "",
            SRS_TEMPLATE.format(parameters.get("srs")) if parameters.get("srs") else "",
            nativeBoundingBox_xml,
            latLonBoundingBox_xml
    )
        if create:
            r = self.post(self.wmslayers_url(workspace,storename=storename), headers=self.contenttype_header("xml"), data=layer_data)
            if r.status_code >= 300:
                raise Exception("Failed to create the wmslayer({}:{}:{}). code = {} , message = {}".format(workspace,storename,layername,r.status_code, r.content))
            logger.debug("Succeed to create the wmslayer({}:{}:{}). ".format(workspace,storename,layername))
            return True
        else:
            r = self.put(self.wmslayer_url(workspace,layername), headers=self.contenttype_header("xml"), data=layer_data)
            if r.status_code >= 300:
                raise Exception("Failed to update the wmslayer({}:{}:{}). code = {} , message = {}".format(workspace,storename,layername,r.status_code, r.content))
            logger.debug("Succeed to update the wmslayer({}:{}:{}). ".format(workspace,storename,layername))
            return False
    

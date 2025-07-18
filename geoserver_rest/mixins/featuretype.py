import logging
import urllib.parse
import os
import tempfile
import xml.etree.ElementTree as ET

from ..exceptions import *
from .. import settings

logger = logging.getLogger(__name__)

FEATURETYPE_VIEWSQL_TEMPLATE = """<?xml version="1.0" encoding="UTF-8"?>
    <featureType>
        <name>{2}</name>
        <namespace>
            <name>{0}</name>
        </namespace>
        <title>{3}</title>
        <abstract>{4}</abstract>
        <keywords>
            {5}
        </keywords>
        <srs>{6}</srs>
        {7}
        {8}
        <enabled>true</enabled>
        <store class="dataStore">
            <name>{0}:{1}</name>
        </store>
        <metadata>
            <entry key="JDBC_VIRTUAL_TABLE">
                <virtualTable>
                    <name>{2}</name>
                    <sql><![CDATA[{9}]]></sql>
                    <escapeSql>{10}</escapeSql>
                    <geometry>
                        <name>{11}</name>
                        <type>{12}</type>
                        <srid>{13}</srid>
                    </geometry>
                </virtualTable>
            </entry>
      </metadata>
    </featureType>
"""
FEATURETYPE_TEMPLATE = """<?xml version="1.0" encoding="UTF-8"?>
    <featureType>
        <name>{2}</name>
        {9}
        <namespace>
            <name>{0}</name>
        </namespace>
        <title>{3}</title>
        <abstract>{4}</abstract>
        <keywords>
            {5}
        </keywords>
        <srs>{6}</srs>
        {7}
        {8}
        <enabled>true</enabled>
        <store class="dataStore">
            <name>{0}:{1}</name>
        </store>
    </featureType>
"""
"""
minx: west longitude
miny: south latitude
maxx: east longitude
maxy: north logitude
"""
NATIVE_BOUNDING_BOX_TEMPLATE = """
        <nativeBoundingBox>
            <minx>{}</minx>
            <miny>{}</miny>
            <maxx>{}</maxx>
            <maxy>{}</maxy>
            <crs>{}</crs>
        </nativeBoundingBox>
"""
"""
minx: west longitude
miny: south latitude
maxx: east longitude
maxy: north logitude
"""
LATLON_BOUNDING_BOX_TEMPLATE = """
        <latLonBoundingBox>
            <minx>{}</minx>
            <miny>{}</miny>
            <maxx>{}</maxx>
            <maxy>{}</maxy>
            <crs>{}</crs>
        </latLonBoundingBox>
"""
LAYER_STYLES_TEMPLATE = """<?xml version="1.0" encoding="UTF-8"?>
<layer>
  {0}
  <styles class="linked-hash-set">
      {1}
  </styles>
</layer>
"""
KEYWORD_TEMPLATE = """<string>{}</string>"""
DEFAULT_STYLE_TEMPLATE = """<defaultStyle><name>{0}:{1}</name><workspace>{0}</workspace></defaultStyle>"""
STYLE_TEMPLATE = """<style><name>{0}:{1}</name><workspace>{0}</workspace></style>"""
NATIVENAME_TEMPLATE = """<nativeName>{}</nativeName>"""

class FeaturetypeMixin(object):
    def featuretypes_url(self,workspace,storename=None):
        if storename:
            return "{0}/rest/workspaces/{1}/datastores/{2}/featuretypes".format(self.geoserver_url,workspace,storename)
        else:
            return "{0}/rest/workspaces/{1}/featuretypes".format(self.geoserver_url,workspace)
    
    def featuretype_url(self,workspace,featurename,storename=None):
        if storename:
            return "{0}/rest/workspaces/{1}/datastores/{2}/featuretypes/{3}".format(self.geoserver_url,workspace,storename,featurename)
        else:
            return "{0}/rest/workspaces/{1}/featuretypes/{2}".format(self.geoserver_url,workspace,featurename)
    
    def layer_styles_url(self,workspace,layername):
        return "{0}/rest/layers/{1}:{2}".format(self.geoserver_url,workspace,layername)

    def featurecount_url(self,workspace,layername):
        return "{}/wfs?service=wfs&version=2.0.0&request=GetFeature&outputFormat=application%2Fxml&typeNames={}%3A{}&resultType=hits".format(self.geoserver_url,workspace,layername)

    def features_url(self,workspace,layername,count=5,bbox=None,srs=None):
        """
        bbox: (minx,miny,maxx,maxy) or {"minx":0,"miny":0,"maxx":0,"maxy":0}
        """
        return "{0}/wfs?service=wfs&version=2.0.0&request=GetFeature&outputFormat=application%2Fjson&typeNames={1}%3A{2}{3}{4}{5}".format(
            self.geoserver_url,
            workspace,
            layername,
            "&count={}".format(count) if count is not None and count > 0 else "",
            "&srsName={}".format(urllib.parse.quote(srs)) if srs else "",
            "&bbox={}".format(",".join(str(bbox[i]) for i in [1,0,3,2]) if isinstance(bbox,(list,tuple)) else ",".join(str(bbox[k]) for k in ("miny","minx","maxy","maxx"))) if bbox else "",
        )

    def wfscapabilities_url(self,version="2.0.0"):
        if version == "2.0.0":
            return "{}/ows?service=WFS&acceptversions=2.0.0&request=GetCapabilities".format(self.geoserver_url)
        elif version == "1.1.0":
            return "{}/ows?service=WFS&version=1.1.0&request=GetCapabilities".format(self.geoserver_url)
        else:
            return "{}/ows?service=WFS&version=1.0.0&request=GetCapabilities".format(self.geoserver_url)

    def get_wfscapabilities(self,version="2.0.0",outputfile=None):
        res = self.get(self.wfscapabilities_url(version=version),headers=self.accept_header("xml"),timeout=settings.GETCAPABILITY_TIMEOUT)
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

    def has_featuretype(self,workspace,layername,storename=None):
        return self.has(self.featuretype_url(workspace,layername,storename=storename))
    
    def get_featuretype(self,workspace,layername,storename=None):
        """
        Return a json object 
        raise ReourceNotFound excepion if not found
        """
        res = self.get(self.featuretype_url(workspace,layername,storename=storename),headers=self.accept_header("json"))
        return res.json()["featureType"]
    
    def get_featurecount(self,workspace,layername,storename=None):
        """
        Return the number of features 
        """
        res = self.get(self.featurecount_url(workspace,layername),headers=self.accept_header("xml"),timeout=settings.GETFEATURE_TIMEOUT)
        try:
            data = ET.fromstring(res.text)
            return int(data.attrib["numberMatched"])
        except Exception as ex:
            raise Exception("Failed to parse the xml data.{}".format(res.text))
    
    def get_features(self,workspace,layername,storename=None,count=5,bbox=None,srs=None):
        """
        bbox: (minx,miny,maxx,maxy) or {"minx":0,"miny":0,"maxx":0,"maxy":0}
        Return the number of features 
        """
        res = self.get(self.features_url(workspace,layername,count=count,bbox=bbox,srs=srs),headers=self.accept_header("json"),timeout=settings.GETFEATURE_TIMEOUT)
        return res.json()

    def list_featuretypes(self,workspace,storename=None):
        """
        Return the list of featuretypes belonging to the workspace and storename(if not empty)
        """
        res = self.get(self.featuretypes_url(workspace,storename),headers=self.accept_header("json"))
        return [str(f["name"]) for f in (res.json().get("featureTypes") or {}).get("featureType") or [] ]
    
    def delete_featuretype(self,workspace,storename,layername):
        if not self.has_featuretype(workspace,layername,storename=storename):
            if self.has_gwclayer(workspace,layername):
                self.delete_gwclayer(workspace,layername)
            return
    
        res = self.delete("{}?recurse=true".format(self.featuretype_url(workspace,layername,storename=storename)))
        logger.debug("Succeed to delete the featuretype({}:{})".format(workspace,layername))
    
    def publish_featuretype(self,workspace,storename,layername,parameters,create=None,recalculate="nativebbox,latlonbbox"):
        """
        Publish a new featuretype
        parameters:
            nativeName: the layer name in the upstream, if empty, use layername as nativeName
            nativeBoundingBox: optional, a native bounding box (minx Longitude , miny Latitude , maxx Longitude , maxy Latitude,crs ); if doesn't exist, looking for bbox.
            latLonBoundingBox: optional, a native bounding box (min Longitude , min Latitude , max Longitude , max Latitude,crs ); if doesn't exist, looking for bbox.
            boundingBox: optinal, a bounding box can be used by nativeBoundingBox and latLonBoundingBox if they doesn't exist
            title: optional, default is layername
            abstract: optional, default is empty
            srs: optional, default is epsg:4326,the spatial reference system
            keywords: optional,
            viewsql: optional, the view sql 
            geometry_column: required for viewsql; the column name of the geometry data
            geometry_type: required for viewsql; the type of the geometry data

        """
        nativeBoundingBox = parameters.get("nativeBoundingBox") or parameters.get("boundingBox")
        latLonBoundingBox = parameters.get("latLonBoundingBox") or parameters.get("boundingBox")
        if parameters.get('viewsql'):
            featuretype_data = FEATURETYPE_VIEWSQL_TEMPLATE.format(
                workspace,
                storename,
                layername,
                self.encode_xmltext(parameters.get("title",layername)), 
                self.encode_xmltext(parameters.get("abstract","")),
                os.linesep.join(KEYWORD_TEMPLATE.format(k) for k in  parameters.get('keywords', [])), 
                parameters.get("srs","EPSG:4326"),
                NATIVE_BOUNDING_BOX_TEMPLATE.format(*nativeBoundingBox) if nativeBoundingBox else "",
                LATLON_BOUNDING_BOX_TEMPLATE.format(*latLonBoundingBox) if latLonBoundingBox else "",
                parameters.get('viewsql'),
                parameters.get("escapeSql","false"),
                parameters.get("geometry_column"),
                parameters.get("geometry_type"),
                parameters.get("srs","EPSG:4326")[5:]
            )
        else:
            featuretype_data = FEATURETYPE_TEMPLATE.format(
                workspace,
                storename,
                layername,
                self.encode_xmltext(parameters.get("title",layername)), 
                self.encode_xmltext(parameters.get("abstract","")),
                os.linesep.join(KEYWORD_TEMPLATE.format(k) for k in  parameters.get('keywords', [])) if parameters.get('keywords') else "", 
                parameters.get("srs","EPSG:4326"),
                NATIVE_BOUNDING_BOX_TEMPLATE.format(*nativeBoundingBox) if nativeBoundingBox else "",
                LATLON_BOUNDING_BOX_TEMPLATE.format(*latLonBoundingBox) if latLonBoundingBox else "",
                NATIVENAME_TEMPLATE.format(parameters.get('nativeName')) if parameters.get('nativeName') else ""
    )
        if create is None:
            featuretype = self.get_featuretype(workspace,layername)
            if featuretype:
                store = featuretype.get("store",{}).get("name","").split(":")[-1]
                if store == storename:
                    create = False
                else:
                    logger.debug("The featuretype({}:{}) is belonging to other store({}), delete it and create it again".format(workspace,layername,store))
                    self.delete_featuretype(workspace,store,layername)
                    create = True
            else:
                create = True
        if create:
            res = self.post(self.featuretypes_url(workspace,storename),headers=self.contenttype_header("xml"),data=featuretype_data)
        else:
            res = self.put("{}?recalculate={}".format(self.featuretype_url(workspace,layername,storename=storename),recalculate or ""),headers=self.contenttype_header("xml"),data=featuretype_data)
    
        logger.debug("Succeed to publish the featuretype({}:{})".format(workspace,layername))
    
    def get_featuretype_styles(self,workspace,layername):
        """
        Return a tuple(default style, [] or alternate styles); 
        Raise ResourceNotFound if layername doesn't exist
        """
        res = self.get(self.layer_styles_url(workspace,layername),headers=self.accept_header("json"))
        data = res.json()
        default_style = data["layer"].get("defaultStyle",{}).get("name",None)
        if isinstance(data["layer"].get("styles",{}).get("style",[]),list):
            return (
                default_style.split(":") if default_style and ":" in default_style else (None,default_style), 
                [d["name"].split(":") if ":" in d["name"] else [None,d["name"]] for d in data["layer"].get("styles",{}).get("style",[])])
        else:
            style = data["layer"].get("styles",{}).get("style")
            if style:
                return (
                    default_style.split(":") if default_style and ":" in default_style else (None,default_style), 
                    [style["name"].split(":") if ":" in style["name"] else [None,style["name"]]])
            else:
                return (
                    default_style.split(":") if default_style and ":" in default_style else (None,default_style), 
                    [])
    
    def set_featuretype_styles(self,workspace,layername,defaultstyle,styles):
        layer_styles_data = LAYER_STYLES_TEMPLATE.format(
            DEFAULT_STYLE_TEMPLATE.format(workspace,defaultstyle) if defaultstyle else "",
            os.linesep.join(STYLE_TEMPLATE.format(workspace,n) for n in styles) if styles else ""
        )
        res = self.put(self.layer_styles_url(workspace,layername),headers=self.contenttype_header("xml"),data=layer_styles_data)
    
        logger.debug("Succeed to set the styles of the layer({}:{}),default_style={}, styles={}".format(workspace,layername,defaultstyle,styles))
    
    def get_featuretype_field(self,featuretypedata,field):
        """
        field:
            name:
            enabled:
            nativename
            title
            abstract
            keywords: list of string
            srs
            nativeBoundingBox: dict(minx,miny,maxx,mzxy,crs)
            latLonBoundingBox: dict(minx,miny,maxx,mzxy,crs)
            namespace/workspace
            datastore related parameters

        Get the wms field from wms json data, returned by get_wmsstore
        """
        if field in ("namespace","workspace"):
            return featuretypedata.get("namespace",{}).get("name")
        elif field == "keywords":
            data = featuretypedata.get("keywords",{}).get("string")
            if not data:
                return []
            else:
                return [data] if isinstance(data,str) else data
        else:
            return featuretypedata.get(field)

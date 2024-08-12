import logging
import os
import xml.etree.ElementTree as ET

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
DEFAULT_STYLE_TEMPLATE = """<defaultStyle><name>{}</name></defaultStyle>"""
STYLE_TEMPLATE = """<style><name>{}</name></style>"""
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
        return "{}/wfs?service=wfs&version=2.0.0&request=GetFeature&outputFormat=application%2Fxml&typeNames={}:{}&resultType=hits".format(self.geoserver_url,workspace,layername)

    def has_featuretype(self,workspace,layername,storename=None):
        return self.has(self.featuretype_url(workspace,layername,storename=storename))
    
    def get_featuretype(self,workspace,layername,storename=None):
        r = self.get(self.featuretype_url(workspace,layername,storename=storename),headers=self.accept_header("json"))
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json()["featureType"]
    
    def get_featurecount(self,workspace,layername,storename=None):
        r = self.get(self.featurecount_url(workspace,layername),headers=self.accept_header("xml"))
        r.raise_for_status()
        try:
            data = ET.fromstring(r.text)
            return int(data.attrib["numberMatched"])
        except Exception as ex:
            raise Exception("Failed to parse the xml data.{}".format(r.text))
    
    def list_featuretypes(self,workspace,storename=None):
        r = self.get(self.featuretypes_url(workspace,storename),headers=self.accept_header("json"))
        if r.status_code >= 300:
            raise Exception("Failed to list the featuretypes in datastore({}:{}). code = {},message = {}".format(workspace,storename,r.status_code, r.content))
    
        return [str(f["name"]) for f in (r.json().get("featureTypes") or {}).get("featureType") or [] ]
    
    def delete_featuretype(self,workspace,storename,layername):
        if not self.has_featuretype(workspace,layername,storename=storename):
            if self.has_gwclayer(workspace,layername):
                self.delete_gwclayer(workspace,layername)
            return
    
        r = self.delete("{}?recurse=true".format(self.featuretype_url(workspace,layername,storename=storename)))
        if r.status_code >= 300:
            raise Exception("Failed to delete the featuretype({}:{}). code = {} , message = {}".format(workspace,layername,r.status_code, r.content))
    
        logger.debug("Succeed to delete the featuretype({}:{})".format(workspace,layername))
    
    def publish_featuretype(self,workspace,storename,layername,parameters,create=None,recalculate="nativebbox,latlonbbox"):
        """
        Publish a new featuretype
        parameters:
            nativeName: the layer name in the upstream, if empty, use layername as nativeName
            nativeBoundingBox: a native bounding box (minx Longitude , miny Latitude , maxx Longitude , maxy Latitude,crs ); if doesn't exist, looking for bbox.
            latLonBoundingBox: a native bounding box (min Longitude , min Latitude , max Longitude , max Latitude,crs ); if doesn't exist, looking for bbox.
            boundingBox: a bounding box can be used by nativeBoundingBox and latLonBoundingBox if they doesn't exist
            srs: the spatial reference system
            viewsql: the view sql 
            geometry_column: the column name of the geometry data
            geometry_type: the type of the geometry data
            nativeName: the native name in the data source

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
            r = self.post(self.featuretypes_url(workspace,storename),headers=self.contenttype_header("xml"),data=featuretype_data)
            if r.status_code >= 300:
                raise Exception("Failed to create the featuretype({}:{}). code = {} , message = {}".format(workspace,layername,r.status_code, r.content))
        else:
            r = self.put("{}?recalculate={}".format(self.featuretype_url(workspace,layername,storename=storename),recalculate or ""),headers=self.contenttype_header("xml"),data=featuretype_data)
            if r.status_code >= 300:
                raise Exception("Failed to update the featuretype({}:{}). code = {} , message = {}".format(workspace,layername,r.status_code, r.content))
    
        logger.debug("Succeed to publish the featuretype({}:{})".format(workspace,layername))
    
    def get_layer_styles(self,workspace,layername):
        """
        Return a tuple(default style, alternate styles); return None if layer doesn't exist
        """
        r = self.get(self.layer_styles_url(workspace,layername),headers=self.accept_header("json"))
        if r.status_code == 200:
            r = r.json()
            default_style = r["layer"].get("defaultStyle",{}).get("name",None)
            if isinstance(r["layer"].get("styles",{}).get("style",[]),list):
                return (
                    default_style.split(":") if default_style and ":" in default_style else (None,default_style), 
                    [d["name"].split(":") if ":" in d["name"] else [None,d["name"]] for d in r["layer"].get("styles",{}).get("style",[])])
            else:
                style = r["layer"].get("styles",{}).get("style")
                return (
                    default_style.split(":") if default_style and ":" in default_style else (None,default_style), 
                    [style["name"].split(":") if ":" in style["name"] else [None,style["name"]]])
        elif r.status_code == 404:
           return None
        else:
            raise Exception("Failed to get styles of the featuretype({}:{}). code = {} , message = {}".format(workspace,layername,r.status_code, r.content))
    
    def set_layer_styles(self,workspace,layername,default_style,styles):
        layer_styles_data = LAYER_STYLES_TEMPLATE.format(
            DEFAULT_STYLE_TEMPLATE.format(default_style) if default_style else "",
            os.linesep.join(STYLE_TEMPLATE.format(n) for n in styles) if styles else ""
        )
        r = self.put(self.layer_styles_url(workspace,layername),headers=self.contenttype_header("xml"),data=layer_styles_data)
        if r.status_code >= 300:
            raise Exception("Failed to set styles of the featuretype({}:{}). code = {} , message = {}".format(workspace,layername,r.status_code, r.content))
    
        logger.debug("Succeed to set the styles of the layer({}:{}),default_style={}, styles={}".format(workspace,layername,default_style,styles))
    

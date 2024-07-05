import logging

logger = logging.getLogger(__name__)

class FeaturetypeMixin(object):
    def featuretypes_url(self,workspace,storename):
        return "{0}/rest/workspaces/{1}/datastores/{2}/featuretypes".format(self.geoserver_url,workspace,storename)
    
    def featuretype_url(self,workspace,featurename,storename=None):
        if storename:
            return "{0}/rest/workspaces/{1}/datastores/{2}/featuretypes/{3}".format(self.geoserver_url,workspace,storename,featurename)
        else:
            return "{0}/rest/workspaces/{1}/featuretypes/{2}".format(self.geoserver_url,workspace,featurename)
    
    def layer_styles_url(self,workspace,layername):
        return "{0}/rest/layers/{1}:{2}".format(self.geoserver_url,workspace,layername)
    
    def has_featuretype(self,workspace,storename,layername):
        return self.has(self.featuretype_url(workspace,layername,storename=storename))
    
    def list_featuretypes(self,workspace,storename):
        r = self.get(self.featuretypes_url(workspace,storename),headers=self.accept_header("json"))
        if r.status_code >= 300:
            raise Exception("Failed to list the featuretypes in datastore({}:{}). code = {},message = {}".format(workspace,storename,r.status_code, r.content))
    
        return [str(f["name"]) for f in (r.json().get("featureTypes") or {}).get("featureType") or [] ]
    
    def delete_featuretype(self,workspace,storename,layername):
        if not self.has_featuretype(workspace,layername,storename=storename):
            if self.has_gwclayer(workspace,layername):
                self.delete_gwclayer(workspace,layername)
            return
    
        r = self.delete("{}?recurse=true".format(self.featuretype_url(workspace,storename,layername)))
        if r.status_code >= 300:
            raise Exception("Failed to delete the featuretype({}:{}). code = {} , message = {}".format(workspace,layername,r.status_code, r.content))
    
        logger.debug("Succeed to delete the featuretype({}:{})".format(workspace,layername))
    
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
    NATIVE_BOUNDING_BOX_TEMPLATE = """
        <nativeBoundingBox>
            <minx>{}</minx>
            <maxx>{}</maxx>
            <miny>{}</miny>
            <maxy>{}</maxy>
            <crs>{}</crs>
        </nativeBoundingBox>
    """
    LATLON_BOUNDING_BOX_TEMPLATE = """
        <latLonBoundingBox>
            <minx>{}</minx>
            <maxx>{}</maxx>
            <miny>{}</miny>
            <maxy>{}</maxy>
            <crs>{}</crs>
        </latLonBoundingBox>
    """
    def publish_featuretype(self,workspace,storename,layername,parameters):
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
            featuretype_data = self.FEATURETYPE_VIEWSQL_TEMPLATE.format(
                workspace,
                storename,
                layername,
                self.encode_xmltext(parameters.get("title",layername)), 
                self.encode_xmltext(parameters.get("abstract","")),
                os.linesep.join("<string>{}</string>".format(k) for k in  parameters.get('keywords', [])), 
                parameters.get("srs","EPSG:4326"),
                self.NATIVE_BOUNDING_BOX_TEMPLATE.format(*nativeBoundingBox) if nativeBoundingBox else "",
                self.LATLON_BOUNDING_BOX_TEMPLATE.format(*latLonBoundingBox) if latLonBoundingBox else "",
                parameters.get('viewsql'),
                parameters.get("escapeSql","false"),
                parameters.get("geometry_column"),
                parameters.get("geometry_type"),
                parameters.get("srs","EPSG:4326")[5:]
            )
        else:
            featuretype_data = self.FEATURETYPE_TEMPLATE.format(
                workspace,
                storename,
                layername,
                self.encode_xmltext(parameters.get("title",layername)), 
                self.encode_xmltext(parameters.get("abstract","")),
                os.linesep.join("<string>{}</string>".format(k) for k in  parameters.get('keywords', [])) if parameters.get('keywords') else "", 
                parameters.get("srs","EPSG:4326"),
                self.NATIVE_BOUNDING_BOX_TEMPLATE.format(*nativeBoundingBox) if nativeBoundingBox else "",
                self.LATLON_BOUNDING_BOX_TEMPLATE.format(*latLonBoundingBox) if latLonBoundingBox else "",
                "<nativeName>{}</nativeName>".format(parameters.get('nativeName')) if parameters.get('nativeName') else ""
    )
    
        r = self.post(self.featuretypes_url(workspace,storename),headers=self.contenttype_header("xml"),data=featuretype_data)
        if r.status_code >= 300:
            raise Exception("Failed to create the featuretype({}:{}). code = {} , message = {}".format(workspace,layername,r.status_code, r.content))
    
        logger.debug("Succeed to publish the featuretype({}:{})".format(workspace,layername))
    
    def get_layer_styles(self,workspace,layername):
        """
        Return a tuple(default style, alternate styles)
        """
        r = self.get(self.layer_styles_url(workspace,layername),headers=self.accept_header("json"))
        if r.status_code == 200:
            r = r.json()
            return (r.get("defaultStyle",{}).get("name",None), [d["name"] for d in r.get("styles",{}).get("style",[])])
        else:
            raise Exception("Failed to get styles of the featuretype({}:{}). code = {} , message = {}".format(workspace,layername,r.status_code, r.content))
    
        LAYER_STYLES_TEMPLATE = """<?xml version="1.0" encoding="UTF-8"?>
<layer>
  {0}
  <styles class="linked-hash-set">
      {1}
  </styles>
</layer>
    """
    def set_layer_styles(self,workspace,layername,default_style,styles):
        layer_styles_data = LAYER_STYLES_TEMPLATE.format(
            "<defaultStyle><name>{}</name></defaultStyle>".format(default_style) if default_style else "",
            os.linesep.join("<style><name>{}</name></style>".format(n) for n in styles) if styles else ""
        )
        r = self.put(self.layer_styles_url(workspace,layername),headers=self.contenttype_header("xml"),data=layer_styles_data)
        if r.status_code >= 300:
            raise Exception("Failed to set styles of the featuretype({}:{}). code = {} , message = {}".format(workspace,layername,r.status_code, r.content))
    
        logger.debug("Succeed to set the styles of the layer({}:{}),default_style={}, styles={}".format(workspace,layername,default_style,styles))
    

import logging
import collections
import os
import string
import requests

logger = logging.getLogger(__name__)

class GeoserverException(

class Geoserver(object):
    def __init__(self,geoserver_url,username,password):
        self.geoserver_url = geoserver_url
        self.username = username
        self.password = password

    def get(self,url,headers=self.accept_header("json")):
        r = requests.get(url , headers=headers, auth=(self.username,self.password))
        if r.status_code >= 500 or (r.status_code >= 300 and r.status_code < 400):
            r.raise_for_status()
        return r

    def has(self,url,headers=self.accept_header("json")):
        r = self.get(url , headers=headers)
        return True if r.status_code == 200 else False

    def post(self,url,data,headers=self.contenttype_header("xml")):
        r = requests.post(url , data=data , headers=headers, auth=(self.username,self.password))
        if r.status_code >= 500 or (r.status_code >= 300 and r.status_code < 400):
            r.raise_for_status()
        return r

    def put(self,url,data,headers=self.contenttype_header("xml")):
        r = requests.put(url , data=data , headers=headers, auth=(self.username,self.password))
        if r.status_code >= 500 or (r.status_code >= 300 and r.status_code < 400):
            r.raise_for_status()
        return r

    def delete(self,url):
        r = requests.delete(url , auth=(self.username,self.password))
        if r.status_code >= 500 or (r.status_code >= 300 and r.status_code < 400):
            r.raise_for_status()
        return r

    @staticmethod
    def encode_xmltext(text):
        if not text:
            return ""
        result = None
        for i in range(len(text)):
            if text[i] not in ['&'] and text[i] in string.printable:
                if result :
                    result += text[i]
            else:
                if result is None:
                    result = text[:i]
                result += "&#{};".format(ord(text[i]))
    
        return result if result else text
    
    @staticmethod
    def contenttype_header(f = "xml"):
        if f == "xml":
            return {"content-type": "application/xml"}
        elif f == "json":
            return {"content-type": "application/json"}
        else:
            raise Exception("Format({}) Not Support".format(f))
    
    @staticmethod
    def accept_header(f = "xml"):
        if f == "xml":
            return {"Accept": "application/xml"}
        elif f == "json":
            return {"Accept": "application/json"}
        elif f == "html":
            return {"Accept": "text/html"}
        else:
            raise Exception("Format({}) Not Support".format(f))

    def featuretypes_url(geoserver_url,workspace,storename):
        return "{0}/rest/workspaces/{1}/datastores/{2}/featuretypes".format(geoserver_url,workspace,storename)
    
    def featuretype_url(geoserver_url,workspace,storename,featurename):
        if storename:
            return "{0}/rest/workspaces/{1}/datastores/{2}/featuretypes/{3}".format(geoserver_url,workspace,storename,featurename)
        else:
            return "{0}/rest/workspaces/{1}/featuretypes/{2}".format(geoserver_url,workspace,featurename)
    
    def gwc_layers_url(geoserver_url):
        return "{0}/gwc/rest/layers".format(geoserver_url)
    
    def gwc_layer_url(geoserver_url,workspace,layername,f=None):
        return "{0}/gwc/rest/layers/{1}:{2}{3}".format(geoserver_url,workspace,layername,".{}".format(f) if f else "")
    
    def gwc_layer_seed_url(geoserver_url,workspace,layername):
        return "{0}/gwc/rest/seed/{1}:{2}.xml".format(geoserver_url,workspace,layername)
    
    def has_featuretype(geoserver_url,username,password,workspace,storename,layername):
        r = requests.get(featuretype_url(geoserver_url,workspace,storename,layername),headers=accept_header(),auth=(username,password))
        return True if r.status_code == 200 else False
    
    def list_featuretypes(geoserver_url,username,password,workspace,storename):
        r = requests.get(featuretypes_url(geoserver_url,workspace,storename),headers=accept_header("json"),  auth=(username,password))
        if r.status_code >= 300:
            raise Exception("Failed to list the featuretypes in datastore({}:{}). code = {},message = {}".format(workspace,storename,r.status_code, r.content))
    
        return [str(f["name"]) for f in (r.json().get("featureTypes") or {}).get("featureType") or [] ]
    
    def publish_featuretype(geoserver_url,username,password,workspace,storename,layername,parameters):
        if parameters.get('viewsql'):
            featuretype_data = """<?xml version="1.0" encoding="UTF-8"?>
    <featureType>
        <name>{2}</name>
        <namespace>
            <name>{0}</name>
        </namespace>
        {3}
        {4}
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
    """.format(
        workspace,
        storename,
        layername,
        "<title>{}</title>".format(encode_xmltext(parameters.get("title"))) if parameters.get("title") else "", 
        "<abstract>{}</abstract>".format(encode_xmltext(parameters.get("abstract"))) if parameters.get("abstract") else "",
        os.linesep.join("<string>{}</string>".format(k) for k in  parameters.get('keywords', [])) if parameters.get('keywords') else "", 
        parameters.get("srs","EPSG:4326"),
        """
        <nativeBoundingBox>
            <minx>{}</minx>
            <maxx>{}</maxx>
            <miny>{}</miny>
            <maxy>{}</maxy>
            <crs>{}</crs>
        </nativeBoundingBox>
    """.format(*parameters["nativeBoundingBox"]) if parameters.get("nativeBoundingBox") else "",
        """
        <latLonBoundingBox>
            <minx>{}</minx>
            <maxx>{}</maxx>
            <miny>{}</miny>
            <maxy>{}</maxy>
            <crs>{}</crs>
        </latLonBoundingBox>
    """.format(*parameters["latLonBoundingBox"]) if parameters.get("latLonBoundingBox") else "",
        parameters.get('viewsql'),
        parameters.get("escapeSql","false"),
        parameters.get("spatial_column"),
        parameters.get("spatial_type"),
        parameters.get("srs","EPSG:4326")[5:]
    )
        else:
            featuretype_data = """<?xml version="1.0" encoding="UTF-8"?>
    <featureType>
        <name>{2}</name>
        {9}
        <namespace>
            <name>{0}</name>
        </namespace>
        {3}
        {4}
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
    """.format(
        workspace,
        storename,
        layername,
        "<title>{}</title>".format(encode_xmltext(parameters.get("title"))) if parameters.get("title") else "",
        "<abstract>{}</abstract>".format(encode_xmltext(parameters.get("abstract"))) if parameters.get("abstract") else "",
        os.linesep.join("<string>{}</string>".format(k) for k in  parameters.get('keywords', [])) if parameters.get('keywords') else "", 
        parameters.get("srs","EPSG:4326"),
        """
        <nativeBoundingBox>
            <minx>{}</minx>
            <maxx>{}</maxx>
            <miny>{}</miny>
            <maxy>{}</maxy>
            <crs>{}</crs>
        </nativeBoundingBox>
    """.format(*parameters["nativeBoundingBox"]) if parameters.get("nativeBoundingBox") else "",
        """
        <latLonBoundingBox>
            <minx>{}</minx>
            <maxx>{}</maxx>
            <miny>{}</miny>
            <maxy>{}</maxy>
            <crs>{}</crs>
        </latLonBoundingBox>
    """.format(*parameters["latLonBoundingBox"]) if parameters.get("latLonBoundingBox") else "",
        "<nativeName>{}</nativeName>".format(parameters.get('table')) if parameters.get('table') else ""
    )
    
        r = requests.post(featuretypes_url(geoserver_url,workspace,storename),headers=contenttype_header("xml"),data=featuretype_data,auth=(username,password))
        if r.status_code >= 300:
            raise Exception("Failed to create the featuretype({}:{}). code = {} , message = {}".format(workspace,layername,r.status_code, r.content))
    
        logger.debug("Succeed to publish the featuretype({}:{})".format(workspace,layername))
    
    def delete_featuretype(geoserver_url,username,password,workspace,storename,layername):
        if not has_featuretype(geoserver_url,username,password,workspace,storename,layername):
            if gwc_has_layer(geoserver_url,username,password,workspace,layername):
                gwc_delete_layer(geoserver_url,username,password,workspace,layername)
            return
    
        r = requests.delete("{}?recurse=true".format(featuretype_url(geoserver_url,workspace,storename,layername)),auth=(username,password))
        if r.status_code >= 300:
            raise Exception("Failed to delete the featuretype({}:{}). code = {} , message = {}".format(workspace,layername,r.status_code, r.content))
    
        logger.debug("Succeed to delete the featuretype({}:{})".format(workspace,layername))
    
    def gwc_has_layer(geoserver_url,username,password,workspace,layername):
        r = requests.get(gwc_layer_url(geoserver_url,workspace,layername,f="json"),headers=accept_header("json"), auth=(username,password))
        return True if r.status_code == 200 else False
    
    def gwc_delete_layer(geoserver_url,username,password,workspace,layername):
        if gwc_has_layer(geoserver_url,username,password,workspace,layername):
            r = requests.delete(gwc_layer_url(geoserver_url,workspace,layername,f="xml"), auth=(username,password))
            if r.status_code >= 300:
                raise Exception("Failed to delete the gwc layer({}:{}). code = {} , message = {}".format(workspace,layername,r.status_code, r.content))
            logger.debug("Succeed to delete the gwc layer({}:{})".format(workspace,layername))
        else:
            logger.debug("The gwc layer({}:{}) doesn't exist".format(workspace,layername))
    
    def gwc_update_layer(geoserver_url,username,password,workspace,layername,parameters):
        layer_data = """<?xml version="1.0" encoding="UTF-8"?>
    <GeoServerLayer>
        <name>{0}:{1}</name>
        <mimeFormats>
            <string>image/png</string>
            <string>image/jpeg</string>
            {2}
        </mimeFormats>
        <enabled>true</enabled>
        <inMemoryCached>true</inMemoryCached>
        <gridSubsets>
            <gridSubset>
                <gridSetName>gda94</gridSetName>
            </gridSubset>
            <gridSubset>
                <gridSetName>mercator</gridSetName>
            </gridSubset>
        </gridSubsets>
        <metaWidthHeight>
            <int>1</int>
            <int>1</int>
        </metaWidthHeight>
        <expireCache>{3}</expireCache>
        <expireClients>{4}</expireClients>
        <parameterFilters>
            <styleParameterFilter>
                <key>STYLES</key>
                <defaultValue></defaultValue>
            </styleParameterFilter>
        </parameterFilters>
        <gutter>100</gutter>
    </GeoServerLayer>
    """.format(
        workspace,
        layername,
        """
            <string>application/json;type=geojson</string>
            <string>application/json;type=topojson</string>
            <string>application/x-protobuf;type=mapbox-vector</string>
            <string>application/json;type=utfgrid</string>
    """ if parameters.get("service_type") == "WFS" else "",
        parameters.get("geoserver_setting",{}).get("server_cache_expire"),
        parameters.get("geoserver_setting",{}).get("client_cache_expire")
    )
    
        r = requests.put(gwc_layer_url(geoserver_url,workspace,layername,f="xml"), auth=(username,password), headers=contenttype_header("xml"), data=layer_data)
            
        if r.status_code >= 300:
            raise Exception("Failed to update the gwc layer({}:{}). code = {} , message = {}".format(workspace,layername,r.status_code, r.content))
    
        logger.debug("Succeed to update the gwc layer({}:{}). ".format(workspace,layername))
    
    def gwc_empty_layer(geoserver_url,username,password,workspace,layername):
        for gridset in ("gda94","mercator"):
            for f in ("image/png","image/jpeg"):
                layer_data = """<?xml version="1.0" encoding="UTF-8"?>
    <seedRequest>
        <name>{0}:{1}</name>
        <gridSetId>{2}</gridSetId>
        <zoomStart>0</zoomStart>
        <zoomStop>24</zoomStop>
        <type>truncate</type>
        <format>{3}</format>
        <threadCount>1</threadCount>
    </seedRequest>
    """.format(
        workspace,
        layername,
        gridset,
        f
    )
                r = requests.post(gwc_layer_seed_url(geoserver_url,workspace,layername),auth=(username,password),headers=collections.ChainMap(accept_header("json"),contenttype_header("xml")), data=layer_data)
                if r.status_code >= 400:
                    raise Exception("Failed to empty the cache of the gwc layer({}:{}). code = {} , message = {}".format(workspace,layername,r.status_code, r.content))
    
        #check whether the task is finished or not.
        finished = False
        while(finished):
            finished = True
            r = requests.get(gwc_layer_url(geoserver_url,workspace,layername), auth=(username,password), headers=accept_header("json"))
            if r.status_code >= 400:
                raise Exception("Failed to empty the cache of the gwc layer({}:{}). code = {} , message = {}".format(workspace,layername,r.status_code, r.content))
    
            tasks=r.json().get("long-array-array",[])
            for t in tasks:
                if t[3] == -1:
                    #aborted
                    raise Exception("Failed to empty the cache of the gwc layer({}:{}). some tasks are aborted".format(workspace,layername))
                elif t[3] in (0,1):
                    finished = False
                    break
            if not finished:
                time.sleep(1)
    
    def list_layers(geoserver_url,username,password):
        """
        List a tuple([(workspace,[(datastore,[featuretypes])])],[(workspace,[(wmsstore,[wmslayers])])],[(workspace,[layergroup])])
        """
        featuretypes = []
        wmslayers = []
        layergroups = []
        for w in list_workspaces(geoserver_url,username,password):
            data = (w,[])
            for s in list_datastores(geoserver_url,username,password,w):
                data[1].append((s,[]))
                for l in list_featuretypes(geoserver_url,username,password,w,s):
                    data[1][-1][1].append(l)
    
                if not data[1][-1][1]:
                    #no layers in datastore,delete the datastore
                    del data[1][-1]
    
            if data[1]:
                featuretypes.append(data)
                
            data = (w,[])
            for s in list_wmsstores(geoserver_url,username,password,w):
                data[1].append((s,[]))
                for l in list_wmslayers(geoserver_url,username,password,w,s):
                    data[1][-1][1].append(l)
                
                if not data[1][-1][1]:
                    #no layers in wmsstore,delete the wmsstore
                    del data[1][-1]
    
            if data[1]:
                wmslayers.append(data)
    
            data = (w,[])
            for l in list_layergroups(geoserver_url,username,password,w):
                data[1].append(l)
    
            if data[1]:
                #no layergroupss in workspace,delete the workspace
                layergroups.append(data)
             
        return (featuretypes,wmslayers,layergroups)
    
    def layers_diff(geoserver1,geoserver2):
        """
        geoserver1 and geoserver2 are tuple(geoserver_url,username,password)
        Return the layers in geoserver1 but not in geoserver2
        """
        geoserver1_layers = list_layers(*geoserver1)
        geoserver2_layers = list_layers(*geoserver2)
        featuretypes = []
        wmslayers = []
        layergroups = []
        #featuretype difference
        for workspace1,stores1 in geoserver1_layers[0]:
            if not stores1:
                continue
            workspace2,stores2 = next((d for d in geoserver2_layers[0] if d[0] == workspace1),(None,None))
            if not stores2 :
                #workspace1 doesn't exist in geoserver2
                featuretypes.append((workspace1,stores1))
                continue
    
            data = (workspace1,[])
            for store1,layers1 in stores1:
                if not layers1:
                    continue
    
                store2,layers2 = next((d for d in stores2 if d[0] == store1),(None,None))
                if not layers2:
                    #store1 doesn't exist in geoserver2
                    data[1].append((store1,layers1))
                    continue
    
                data[1].append((store1,[]))
                for layer1 in layers1:
                    if layer1 in layers2:
                        continue
                    data[1][-1][1].append(layer1)
    
                if not data[1][-1][1]:
                    #all layers in datastore in geoserver1 exist in geoserver2 too. ,delete the datastore
                    del data[1][-1]
    
            if data[1]:
                featuretypes.append(data)
                
    
        #wmslayer difference
        for workspace1,stores1 in geoserver1_layers[1]:
            if not stores1:
                continue
            workspace2,stores2 = next((d for d in geoserver2_layers[1] if d[0] == workspace1),(None,None))
            if not stores2 :
                #workspace1 doesn't exist in geoserver2
                wmslayers.append((workspace1,stores1))
                continue
    
            data = (workspace1,[])
            for store1,layers1 in stores1:
                if not layers1:
                    continue
                store2,layers2 = next((d for d in stores2 if d[0] == store1),(None,None))
                if not layers2:
                    #store1 doesn't exist in geoserver2
                    data[1].append((store1,layers1))
                    continue
    
                data[1].append((store1,[]))
                for layer1 in layers1:
                    if layer1 in layers2:
                        continue
                    data[1][-1][1].append(layer1)
    
                if not data[1][-1][1]:
                    #all layers in datastore in geoserver1 exist in geoserver2 too. ,delete the datastore
                    del data[1][-1]
                
            if data[1]:
                wmslayers.append(data)
    
        #layergroup difference
        for workspace1,groups1 in geoserver1_layers[2]:
            if not groups1:
                continue
            workspace2,groups2 = next((d for d in geoserver2_layers[2] if d[0] == workspace1),(None,None))
            if not workspace2 :
                #workspace1 doesn't exist in geoserver2
                layergroups.append((workspace1,groups1))
                continue
    
            for group1 in groups1:
                if group1 in groups2:
                    continue
                if not layergroups or layergroups[-1][0] != workspace1:
                    layergroups.append((workspace1,[group1]))
                else:
                    layergroups[-1][1].append(group1)
    
        return (featuretypes,wmslayers,layergroups)
                

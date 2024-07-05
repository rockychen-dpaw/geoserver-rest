import logging
import itertools
import collections

logger = logging.getLogger(__name__)

class GWCMixin(object):
    def gwclayers_url(self):
        return "{0}/gwc/rest/layers".format(self.geoserver_url)
    
    def gwclayer_url(workspace,layername,f=None):
        return "{0}/gwc/rest/layers/{1}:{2}{3}".format(self.geoserver_url,workspace,layername,".{}".format(f) if f else "")
    
    def gwclayer_seed_url(workspace,layername):
        return "{0}/gwc/rest/seed/{1}:{2}.xml".format(self.geoserver_url,workspace,layername)
    
    def has_gwclayer(workspace,layername):
        return self.has(self.gwclayer_url(workspace,layername,f="json"))
    
    def delete_gwclayer(self,workspace,layername):
        if self.has_gwclayer(workspace,layername):
            r = self.delete(self.gwclayer_url(workspace,layername,f="xml"))
            if r.status_code >= 300:
                raise Exception("Failed to delete the gwc layer({}:{}). code = {} , message = {}".format(workspace,layername,r.status_code, r.content))
            logger.debug("Succeed to delete the gwc layer({}:{})".format(workspace,layername))
        else:
            logger.debug("The gwc layer({}:{}) doesn't exist".format(workspace,layername))

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
    def update_gwclayer(self,workspace,layername,parameters,is_featuretype=None):
        if is_featuretype is None:
            is_featuretype = self.has_featuretype(workspace,layername)
        formats = parameters.get("mimeFormats",["image/png","image/jpeg"])
        if is_featuretype:
            formats = itertools.chain(formats,["application/json;type=geojson","application/json;type=topojson","application/x-protobuf;type=mapbox-vector","application/json;type=utfgrid"])
        layer_data = self.LAYER_DATA_TEMPLATE.format(
          workspace,
          layername,
        "".join("<string>{}</string>".format(f) for f in formats),
        "".join("<gridSubset>{}</gridSubset>".format(f) for f in parameters.get("gridSubsets",["gda94","mercator"])),
        parameters.get("metaWidth",1),
        parameters.get("metaHeight",1),
        parameters.get("expireCache",0),
        parameters.get("expireClients",0),
        parameters.get("gutter",100)
    )
    
        r = self.put(self.gwclayer_url(workspace,layername,f="xml"), headers=self.contenttype_header("xml"), data=layer_data)
            
        if r.status_code >= 300:
            raise Exception("Failed to update the gwc layer({}:{}). code = {} , message = {}".format(workspace,layername,r.status_code, r.content))
    
        logger.debug("Succeed to update the gwc layer({}:{}). ".format(workspace,layername))
    
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
    def empty_gwclayer(self,workspace,layername,gitsubsets=["gda94","mercator"],formats=["image/png","image/jpeg"]):
        for gridset in gridsubsets:
            for f in formats:
                layer_data = self.EMPTY_LAYER_TEMPLATE.format(
                    workspace,
                    layername,
                    gridset,
                    f
                )
                r = self.post(self.gwclayer_seed_url(workspace,layername),headers=collections.ChainMap(self.accept_header("json"),self.contenttype_header("xml")), data=layer_data)
                if r.status_code >= 400:
                    raise Exception("Failed to empty the cache of the gwc layer({}:{}). code = {} , message = {}".format(workspace,layername,r.status_code, r.content))
    
        #check whether the task is finished or not.
        finished = False
        while(finished):
            finished = True
            r = self.get(self.gwclayer_url(workspace,layername), headers=self.accept_header("json"))
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
    

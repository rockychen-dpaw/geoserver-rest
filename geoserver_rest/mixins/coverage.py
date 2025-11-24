import logging
import os
import time
import tempfile
import traceback

from ..exceptions import *
from .. import settings

logger = logging.getLogger(__name__)

class CoverageMixin(object):
    def coverages_url(self,workspace,storename=None):
        if storename:
            return "{0}/rest/workspaces/{1}/coveragestores/{2}/coverages".format(self.geoserver_url,workspace,storename)
        else:
            return "{0}/rest/workspaces/{1}/coverages".format(self.geoserver_url,workspace)
    
    def coverage_url(self,workspace,layername,storename="",format="json"):
        if storename:
            return "{0}/rest/workspaces/{1}/coveragestores/{2}/coverages/{3}.{4}".format(self.geoserver_url,workspace,storename,layername,format)
        else:
            return "{0}/rest/workspaces/{1}/coverages/{2}.{3}".format(self.geoserver_url,workspace,layername,format)
    
    def coveragecapabilities_url(self,version="2.0.1"):
        if version == "2.0.1":
            return "{}/ows?service=WCS&acceptversions=2.0.1&request=GetCapabilities".format(self.geoserver_url)
        else:
            return "{}/ows?service=WCS&version={}&request=GetCapabilities".format(self.geoserver_url,version)

    def get_coveragecapabilities(self,version="1.3.0",outputfile=None):
        while True:
            attempts = 0
            try:
                attempts += 1
                res = self.get(self.coveragecapabilities_url(version=version),headers=self.accept_header("xml"),timeout=settings.GETCAPABILITY_TIMEOUT)
                break
            except Exception as ex:
                if attempts > 10:
                    raise
                elif "InvalidChunkLength" in str(ex):
                    time.sleep(1)
                    continue
                else:
                    raise

        if outputfile:
            output = open(outputfile,'wb')
        else:
            output = tempfile.NamedTemporaryFile(
                mode='wb',
                prefix="gscoveragecapabilities_",
                suffix=".xml",
                delete = False, 
                delete_on_close = False
            )
            outputfile = output.name

        try:
            for data in res.iter_content(chunk_size = 1024):
                output.write(data)
            if attempts == 1:
                logger.debug("Coverage capabilities was saved to {}".format(outputfile))
            else:
                logger.debug("Coverage capabilities was saved to {}, but tried {} times.".format(outputfile,attempts))
            return outputfile
        finally:
            output.close()

    def has_coverage(self,workspace,layername,storename=None):
        return self.has(self.coverage_url(workspace,layername,storename=storename,format="json"),headers=self.accept_header("json"))
    
    def get_coverage(self,workspace,layername,storename=None):
        """
        Return a json object if exists; 
        Raise ResourceNotFound if not found
        """
        res = self.get(self.coverage_url(workspace,layername,storename=storename,format="json"),headers=self.accept_header("json"))
        return res.json()["coverage"]
    
    def list_coverages(self,workspace,storename):
        """
        Return the list of layers in the store if storename is not null;otherwise return all layers in the workspace
        """
        res = self.get(self.coverages_url(workspace,storename),headers=self.accept_header("json"))
        return [str(l["name"]) for l in (res.json().get("coverages") or {}).get("coverage") or [] ]
    
    def get_coverage_field(self,layerdata,field):
        """
        field:
            name:
            enabled:
            nativename
            title
            abstract
            description
            srs
            nativeBoundingBox: dict(minx,miny,maxx,mzxy,crs)
            latLonBoundingBox: dict(minx,miny,maxx,mzxy,crs)
            namespace/workspace

        Get the wms field from wms json data, returned by get_wmsstore
        """
        if field in ("namespace","workspace"):
            return layerdata.get("namespace",{}).get("name")
        elif field == "keywords":
            data = layerdata.get("keywords",{}).get("string")
            if not data:
                return []
            else:
                return [data] if isinstance(data,str) else data
        else:
            return layerdata.get(field)

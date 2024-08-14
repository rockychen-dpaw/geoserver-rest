import urllib.parse
import tempfile
import logging

from ..exceptions import *

logger = logging.getLogger(__name__)

class WMSServiceMixin(object):
    def map_url(self,workspace,layername,bbox,version="1.1.0",srs="EPSG:4326",width=1024,height=1024,format="image/jpeg",style=""):
        """
        bbox: [minx,miny,maxx,maxy]
        """
        parameters = "service=WMS&version={0}&request=GetMap&layers={1}%3A{2}&bbox={4}&width={7}&height={8}&srs={3}&styles={5}&format={6}".format(
            version,workspace,layername,srs,"%2C".join(str(d) for d in bbox),style or "",urllib.parse.quote(format),width,height
        )
        return "{0}/{1}/wms?{2}".format(self.geoserver_url,workspace,parameters)


    def get_map(self,workspace,layername,bbox,version="1.1.0",srs="EPSG:4326",width=1024,height=1024,format="image/jpeg",style="",outputfile=None):
        """
        outputfile: a temporary file will be created if outputfile is None, the client has the responsibility to delete the outputfile,
        If succeed, save the image to outputfile
        """
        url = self.map_url(workspace,layername,bbox,version=version,srs=srs,width=width,height=height,format=format,style=style)
        logger.debug("get map url = {}".format(url))
        res = self.get(url,headers=self.accept_header(format),timeout=600)
        if res.headers.get("content-type") != format:
            if res.headers.get("content-type","").startswith("text/"):
                raise GetMapFailed("Failed to get the map of layer({}:{}).{}".format(workspace,layername,res.text),res)
            else:
                raise GetMapFailed("Failed to get the map of layer({}:{}).Expect '{}', but got '{}'".format(workspace,layername,format,res.headers.get("content-type","")),res)
        if outputfile:
            output = open(outputfile,'wb')
        else:
            output = tempfile.NamedTemporaryFile(
                mode='wb',
                prefix="gswms_",
                suffix=".{}".format(format.rsplit("/",1)[1] if "/" in format else format),
                delete = False,
                delete_on_close = False
            )
            outputfile = output.name
        try:
            for data in res.iter_content(chunk_size = 1024):
                output.write(data)

            logger.debug("WMS image was saved to {}".format(outputfile))
            return outputfile
        finally:
            output.close()


import logging

from . import settings
from .geoserver import Geoserver

logger = logging.getLogger(__name__)

class Geocluster():
    def __init__(self,geoadmin_url,username,password,headers=None):
        super().__init__(geoadmin_url,username,password,headers=headers)



    def get(self,url,headers=GeoserverUtils.accept_header("json"),timeout=settings.REQUEST_TIMEOUT,error_handler=None):
        logger.debug("GET {}".format(url))
        if self.headers:
            if headers:
                headers = collections.ChainMap(headers,self.headers)
            else:
                headers = self.headers
        res = requests.get(url , headers=headers, auth=(self.username,self.password),timeout=timeout)
        (error_handler or self._handle_response_error)(res)
        return res

    def has(self,url,headers=GeoserverUtils.accept_header("json"),timeout=settings.REQUEST_TIMEOUT,error_handler=None):
        try:
            r = self.get(url , headers=headers,timeout=timeout,error_handler=error_handler)
            return True if r.status_code == 200 else False
        except ResourceNotFound as ex:
            return False

    def post(self,url,data,headers=GeoserverUtils.contenttype_header("xml"),timeout=settings.REQUEST_TIMEOUT,error_handler=None):
        logger.debug("POST {}".format(url))
        if self.headers:
            if headers:
                headers = collections.ChainMap(headers,self.headers)
            else:
                headers = self.headers
        res = requests.post(url , data=data , headers=headers, auth=(self.username,self.password),timeout=timeout)
        (error_handler or self._handle_response_error)(res)
        return res

    def put(self,url,data,headers=GeoserverUtils.contenttype_header("xml"),timeout=settings.REQUEST_TIMEOUT,error_handler=None):
        logger.debug("PUT {}".format(url))
        if self.headers:
            if headers:
                headers = collections.ChainMap(headers,self.headers)
            else:
                headers = self.headers
        res = requests.put(url , data=data , headers=headers, auth=(self.username,self.password),timeout=timeout)
        (error_handler or self._handle_response_error)(res)
        return res

    def delete(self,url,headers=None,timeout=settings.REQUEST_TIMEOUT,error_handler=None):
        logger.debug("DELETE {}".format(url))
        if self.headers:
            if headers:
                headers = collections.ChainMap(headers,self.headers)
            else:
                headers = self.headers


        res = requests.delete(url , auth=(self.username,self.password),headers=headers,timeout=timeout)
        (error_handler or self._handle_response_error)(res)
        return res

    def all_layers(self):
        """
        List a tuple([(workspace,[(datastore,[featuretypes])])],[(workspace,[(wmsstore,[wmslayers])])],[(workspace,[layergroup])])
        """
        featuretypes = []
        wmslayers = []
        layergroups = []
        for w in self.list_workspaces():
            data = (w,[])
            for s in self.list_datastores(w):
                data[1].append((s,[]))
                for l in self.list_featuretypes(w,s):
                    data[1][-1][1].append(l)
    
                if not data[1][-1][1]:
                    #no layers in datastore,delete the datastore
                    del data[1][-1]
    
            if data[1]:
                featuretypes.append(data)
                
            data = (w,[])
            for s in self.list_wmsstores(w):
                data[1].append((s,[]))
                for l in self.list_wmslayers(w,s):
                    data[1][-1][1].append(l)
                
                if not data[1][-1][1]:
                    #no layers in wmsstore,delete the wmsstore
                    del data[1][-1]
    
            if data[1]:
                wmslayers.append(data)
    
            data = (w,[])
            for l in self.list_layergroups(w):
                data[1].append(l)
    
            if data[1]:
                #no layergroupss in workspace,delete the workspace
                layergroups.append(data)
             
        return (featuretypes,wmslayers,layergroups)

import logging
import collections
import os
import string
import requests
from .mixins import *

logger = logging.getLogger(__name__)

class GeoserverException(Exception):
    pass

class GeoserverUtils(object):
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
        elif f == "jpeg":
            return {"Accept": "image/jpeg"}
        elif f == "png":
            return {"Accept": "image/png"}
        else:
            raise Exception("Format({}) Not Support".format(f))

class Geoserver(AboutMixin,DatastoreMixin,FeaturetypeMixin,GWCMixin,LayergroupMixin,ReloadMixin,SecurityMixin,StyleMixin,WMSLayerMixin,WMSStoreMixin,WorkspaceMixin,UsergroupMixin,GeoserverUtils):
    def __init__(self,geoserver_url,username,password,headers=None):
        assert geoserver_url,"Geoserver URL is not configured"
        assert username,"Geoserver user is not configured"
        assert password,"Geoserver user password is not configured"
        self.geoserver_url = geoserver_url
        self.username = username
        self.password = password
        self.headers = headers

    def get(self,url,headers=GeoserverUtils.accept_header("json"),raise_exception=True,timeout=30):
        if self.headers:
            headers = collections.ChainMap(headers,self.headers)
        r = requests.get(url , headers=headers, auth=(self.username,self.password),timeout=timeout)
        if raise_exception:
            if r.status_code >= 500 or (r.status_code >= 300 and r.status_code < 400):
                logger.error("Failed to access the url({}). code = {} , message = {}".format(url,r.status_code, r.content))
                r.raise_for_status()
        return r

    def has(self,url,headers=GeoserverUtils.accept_header("json"),timeout=30):
        r = self.get(url , headers=headers,timeout=timeout)
        return True if r.status_code == 200 else False

    def post(self,url,data,headers=GeoserverUtils.contenttype_header("xml"),timeout=30):
        if self.headers:
            headers = collections.ChainMap(headers,self.headers)
        r = requests.post(url , data=data , headers=headers, auth=(self.username,self.password),timeout=timeout)
        if r.status_code >= 500 or (r.status_code >= 300 and r.status_code < 400):
            logger.error("Failed to post the url({}). code = {} , message = {}".format(url,r.status_code, r.content))
            r.raise_for_status()
        return r

    def put(self,url,data,headers=GeoserverUtils.contenttype_header("xml"),timeout=30):
        if self.headers:
            headers = collections.ChainMap(headers,self.headers)
        r = requests.put(url , data=data , headers=headers, auth=(self.username,self.password),timeout=timeout)
        if r.status_code >= 500 or (r.status_code >= 300 and r.status_code < 400):
            logger.error("Failed to put the url({}). code = {} , message = {}".format(url,r.status_code, r.content))
            r.raise_for_status()
        return r

    def delete(self,url,headers=None,timeout=30):
        if self.headers:
            headers = collections.ChainMap(headers,self.headers)
        r = requests.delete(url , auth=(self.username,self.password),headers=headers,timeout=timeout)
        if r.status_code >= 500 or (r.status_code >= 300 and r.status_code < 400):
            logger.error("Failed to delete the url({}). code = {} , message = {}".format(url,r.status_code, r.content))
            r.raise_for_status()
        return r

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

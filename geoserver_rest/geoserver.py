import logging
import collections
import os
import string
import requests
import urllib.parse

from .mixins import *
from .exceptions import *
from . import settings

logger = logging.getLogger(__name__)

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
    def urlencode(s):
        return urllib.parse.quote(s)

    @staticmethod
    def contenttype_header(f = "xml"):
        if f == "xml":
            return {"content-type": "application/xml"}
        elif f == "json":
            return {"content-type": "application/json"}
        elif f == "jpeg":
            return {"content-type": "image/jpeg"}
        elif f == "png":
            return {"content-type": "image/png"}
        elif f in ("gpkg","geopackage"):
            return {"content-type": "application/x-sqlite3"}
        elif "/" in f:
            return {"content-type": f}
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
        elif "/" in f:
            return {"Accept": f}
        else:
            raise Exception("Format({}) Not Support".format(f))

    def _handle_response_error(self,res):
        if res.status_code >= 300 and res.status_code < 400:
            raise UnauthorizedException(
                """URL: {0}
{1}: No permission to access this resource.""".format(res.request.url,res.status_code),
                response=res
            )
        elif res.status_code == 401:
            raise UnauthenticatedException(res)
        elif res.status_code == 403:
            raise UnauthorizedException(res)
        elif res.status_code == 404:
            raise ResourceNotFound(res)
        elif res.status_code == 405:
            raise HttpMethodNotSupport(res)
        elif res.status_code >= 400:
            try:
                res.raise_for_status()
            except Exception as ex:
                msg = """URL: {0}
{1}
{2}""".format(res.request.url,str(ex),res.text)
                raise ex.__class__(msg,response=res)

class Geoserver(WMSServiceMixin,AboutMixin,DatastoreMixin,FeaturetypeMixin,GWCMixin,LayergroupMixin,ReloadMixin,SecurityMixin,StyleMixin,WMSLayerMixin,WMSStoreMixin,WorkspaceMixin,UsergroupMixin,CoverageStoreMixin,CoverageMixin,RolesMixin,GeoserverUtils):
    def __init__(self,geoserver_url,username,password,headers=None,ssl_verify=True):
        assert geoserver_url,"Geoserver URL is not configured"
        assert username,"Geoserver user is not configured"
        assert password,"Geoserver user password is not configured"
        self.geoserver_url = geoserver_url
        self.username = username
        self.password = password
        self.headers = headers
        self.ssl_verify = ssl_verify


    def __str__(self):
        return self.geoserver_url


    def get(self,url,headers=GeoserverUtils.accept_header("json"),timeout=settings.REQUEST_TIMEOUT,error_handler=None):
        logger.debug("GET {}".format(url))
        if self.headers:
            if headers:
                headers = collections.ChainMap(headers,self.headers)
            else:
                headers = self.headers
        res = requests.get(url , headers=headers, auth=(self.username,self.password),timeout=timeout,verify=self.ssl_verify)
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
        res = requests.post(url , data=data , headers=headers, auth=(self.username,self.password),timeout=timeout,verify=self.ssl_verify)
        (error_handler or self._handle_response_error)(res)
        return res

    def put(self,url,data,headers=GeoserverUtils.contenttype_header("xml"),timeout=settings.REQUEST_TIMEOUT,error_handler=None):
        logger.debug("PUT {}".format(url))
        if self.headers:
            if headers:
                headers = collections.ChainMap(headers,self.headers)
            else:
                headers = self.headers
        res = requests.put(url , data=data , headers=headers, auth=(self.username,self.password),timeout=timeout,verify=self.ssl_verify)
        (error_handler or self._handle_response_error)(res)
        return res

    def delete(self,url,headers=None,timeout=settings.REQUEST_TIMEOUT,error_handler=None):
        logger.debug("DELETE {}".format(url))
        if self.headers:
            if headers:
                headers = collections.ChainMap(headers,self.headers)
            else:
                headers = self.headers


        res = requests.delete(url , auth=(self.username,self.password),headers=headers,timeout=timeout,verify=self.ssl_verify)
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


def get_default_geoserver():
    geoserver_url = os.environ["GEOSERVER_URL"]
    geoserver_user = os.environ["GEOSERVER_USER"]
    geoserver_password = os.environ["GEOSERVER_PASSWORD"]
    geoserver_ssl_verify = os.environ.get("GEOSERVER_SSL_VERIFY","true").lower() == "true"

    return Geoserver(geoserver_url,geoserver_user,geoserver_password,headers=settings.GET_REQUEST_HEADERS("GEOSERVER_REQUEST_HEADERS"),ssl_verify=geoserver_ssl_verify)


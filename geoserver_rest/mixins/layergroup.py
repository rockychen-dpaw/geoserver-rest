import logging
import requests
import os

from ..exceptions import *

logger = logging.getLogger(__name__)

GROUP_TEMPLATE = """<?xml version="1.0" encoding="UTF-8"?>
<layerGroup>
    <name>{1}</name>
    <mode>SINGLE</mode>
    <title>{2}</title>
    <abstract>{3}</abstract>
    <workspace>
        <name>{0}</name>
    </workspace>
    <publishables>
        {5}
    </publishables>
    <keywords>
        {4}
    </keywords>
</layerGroup>
"""
PUBLISHED_TEMPLATE = """
        <published type="{}">
            <name>{}:{}</name>
        </published>
"""
KEYWORD_TEMPLATE = """<string>{}</string>"""

class LayergroupMixin(object):
    def layergroups_url(self,workspace):
        return "{0}/rest/workspaces/{1}/layergroups".format(self.geoserver_url,workspace)
    
    def layergroup_url(self,workspace,groupname):
        return "{0}/rest/workspaces/{1}/layergroups/{2}".format(self.geoserver_url,workspace,groupname)
    
    def has_layergroup(self,workspace,groupname):
        return self.has(self.layergroup_url(workspace,groupname),headers=self.accept_header("json"))
    
    def get_layergroup(self,workspace,groupname):
        """
        Reutn the layergroup data 
        {
            "name": "testgs4unitest",
            "mode": "SINGLE",
            "title": "test layergroup for unitest",
            "workspace": {
                "name": "testws4unitest"
            },
            "publishables": {
                "published": [
                    {
                        "@type": "layer",
                        "name": "testws4unitest:testft4unitest",
                        "href": "https://kmiadmin-uat.dbca.wa.gov.au/geoserver/rest/workspaces/testws4unitest/layers/testft4unitest.json"
                    },
                    {
                        "@type": "layer",
                        "name": "testws4unitest:testview4unitest",
                        "href": "https://kmiadmin-uat.dbca.wa.gov.au/geoserver/rest/workspaces/testws4unitest/layers/testview4unitest.json"
                    }
                ]
            },
            "styles": {
                "style": [
                    "",
                    ""
                ]
            },
            "bounds": {
                "minx": 112.865641255,
                "maxx": 129.019915761,
                "miny": -35.241850284,
                "maxy": -13.507619797,
                "crs": "EPSG:4326"
            },
            "keywords": {
                "string": "unitest" or []
            },
            "dateCreated": "2025-06-18 00:47:24.331 UTC"
        }
        """
        try:
            res = self.get(self.layergroup_url(workspace,groupname),headers=self.accept_header("json"))
            return res.json()["layerGroup"]
        except ResourceNotFound as ex:
            return None
    
    def get_layergroupfield(self,layergroupdict,field):
        """
        field: 
           layers: Return list of tuple (type,workspace,name)
           workspace: 
           name:
           title
           keywords: Return list of keywords. or empty list
           bounds: a dict with keys: minx miny, maxx, maxy

        Return the value of field from layergroup json data
        """
        if field == "layers":
            return [ (layer["@type"],*layer["name"].split(":",1)) if ":" in layer["name"] else (layer["@type"],layergroupdict["workspace"]["name"],layer["name"]) for layer in layergroupdict.get("publishables",{}).get("published",[])]
        elif field == "workspace":
            return layergroupdict["workspace"]["name"]
        elif field == "keywords":
            result = layergroupdict["keywords"]["string"]
            if result:
                return [result] if isinstance(result,str) else result
            else:
                return []
        elif field == "bounds":
            return layergroupdict["bounds"]
        elif field == "name":
            return layergroupdict["name"]
        elif field == "title":
            return layergroupdict["title"]
        else:
            raise Exception("Not Support")
    
    def list_layergroups(self,workspace):
        """
        Return the list of layergroup name without workspace name

        """
        res = self.get(self.layergroups_url(workspace),headers=self.accept_header("json"))
        return [str(g["name"]) for g in (res.json().get("layerGroups") or {}).get("layerGroup") or [] ]
    
    
    def delete_layergroup(self,workspace,groupname):
        """
        Return True if deleted;otherwise return False if doesn't exist before
        """
        if not self.has_layergroup(workspace,groupname):
            logger.debug("The layergroup({}:{}) doesn't exist".format(workspace,groupname))
            return False
    
        res = self.delete(self.layergroup_url(workspace,groupname))
        logger.debug("Succeed to delete the layergroup({}:{})".format(workspace,groupname))
        return True
    
    def update_layergroup(self,workspace,groupname,parameters,create=None):
        """
        parameters:
            title: optional
            abstract: optional
            keywords: optional
            layers:[
                {"type":"group|layer","name":"test","workspace":"test"}
                {"type":"group|layer","name":"test","workspace":"test"}
            ]
                
        Return True if created;otherwise return False if updated
        """
        group_data = GROUP_TEMPLATE.format(
            workspace,
            groupname,
            self.encode_xmltext(parameters.get("title")),
            self.encode_xmltext(parameters.get("abstract")),
            os.linesep.join(KEYWORD_TEMPLATE.format(k) for k in  parameters.get('keywords', [])), 
            os.linesep.join(PUBLISHED_TEMPLATE.format(
                "layerGroup" if layer["type"] == "group" else "layer",
                layer["workspace"],
                layer["name"]) for layer in parameters["layers"]
        ))
        if create is None:
            if self.has_layergroup(workspace,groupname):
                create = False
            else:
                create = True
        try:
            if create:
                #layer doesn't exist
                res = self.post(self.layergroups_url(workspace), headers=self.contenttype_header("xml"), data=group_data)
                logger.debug("Succeed to create the layergroup({}:{}). ".format(workspace,groupname))
                return True
            else:
                res = self.put(self.layergroup_url(workspace,groupname), headers=self.contenttype_header("xml"), data=group_data)
                logger.debug("Succeed to update the layergroup({}:{}). ".format(workspace,groupname))
                return False
        except requests.RequestException as ex:
            if create == False:
                #update group({0}) failed, try to delete and readd it"
                self.delete_layergroup(workspace,groupname)
                return self.update_layergroup(workspace,groupname,parameters)
            else:
                raise ex
    

import logging

logger = logging.getLogger(__name__)

GROUP_TEMPLATE = """<?xml version="1.0" encoding="UTF-8"?>
<layerGroup>
    <name>{1}</name>
    <mode>SINGLE</mode>
    <title>{}</title>
    <abstract>{}</abstract>
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
        return self.has(self.layergroup_url(workspace,groupname),headers=accept_header("json"))
    
    def list_layergroups(self,workspace):
        r = self.get(self.layergroups_url(workspace),headers=accept_header("json"))
        if r.status_code >= 300:
            raise Exception("Failed to list the layergroups in workspace({}:{}). code = {},message = {}".format(workspace,r.status_code, r.content))
    
        return [str(g["name"]) for g in (r.json().get("layerGroups") or {}).get("layerGroup") or [] ]
    
    
    def delete_layergroup(self,workspace,groupname):
        """
        Return True if deleted;otherwise return False if doesn't exist before
        """
        if not self.has_layergroup(workspace,groupname):
            logger.debug("The layergroup({}:{}) doesn't exist".format(workspace,groupname))
            return False
    
        r = self.delete(self.layergroup_url(workspace,groupname))
        if r.status_code >= 300:
            raise Exception("Failed to delete layergroup({}:{}). code = {} , message = {}".format(workspace,groupname,r.status_code, r.content))
    
        logger.debug("Succeed to delete the layergroup({}:{})".format(workspace,groupname))
        return True
    
    def update_layergroup(geoserver_url,username,password,workspace,groupname,parameters):
        """
        parameters:
            layers:
                {"type":"group|layer","name":"test","workspace":"test"}
                {"type":"group|layer","name":"test","workspace":"test"}
                
        Return True if created;otherwise return False if updated
        """
        group_data = GROUP_TEMPLATE.format(
            workspace,
            groupname,
            self.encode_xmltext(parameters.get("title")),
            encode_xmltext(parameters.get("abstract")),
            os.linesep.join(KEYWORD_TEMPLATE.format(k) for k in  parameters.get('keywords', [])), 
            os.linesep.join(PUBLISHED_TEMPLATE.format(
                "layerGroup" if layer["type"] == "group" else "layer",
                layer["workspace"],
                layer["name"]) for layer in parameters.get("layers",{})
        ))
        if self.has_layergroup(workspace,groupname):
            r = self.put(self.layergroup_url(workspace,groupname), headers=self.contenttype_header("xml"), data=group_data)
            create = False
        else:
            #layer doesn't exist
            r = self.post(self.layergroups_url(workspace), headers=self.contenttype_header("xml"), data=group_data)
            create = True
    
        if r.status_code >= 300:
            if r.status_code >= 400 and create == False:
                #update group({0}) failed, try to delete and readd it"
                self.delete_layergroup(workspace,groupname)
                return self.update_layergroup(workspace,groupname,parameters)
            else:
                raise Exception("Failed to {} the layergroup({}:{}). code = {} , message = {}".format(
                    "create" if create else "update",
                    workspace,
                    groupname,
                    r.status_code, 
                    r.content
                ))
    
        if create:
            logger.debug("Succeed to create the layergroup({}:{}). ".format(workspace,groupname))
            return True
        else:
            logger.debug("Succeed to update the layergroup({}:{}). ".format(workspace,groupname))
            return False
    

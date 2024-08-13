import logging
import requests

from ..exceptions import *

logger = logging.getLogger(__name__)

WORKSPACE_TEMPLATE = """<?xml version="1.0" encoding="UTF-8"?>
<workspace>
    <name>{}</name>
</workspace>
"""
class WorkspaceMixin(object):
    def workspaces_url(self):
        return "{0}/rest/workspaces".format(self.geoserver_url)
    
    def workspace_url(self,workspace):
        return "{0}/rest/workspaces/{1}".format(self.geoserver_url,workspace)
    
    def has_workspace(self,workspace):
        return self.has(self.workspace_url(workspace))
    
    def list_workspaces(self):
        """
        Return unordered workspace list.
        """
        res = self.get(self.workspaces_url(),headers=self.accept_header("json"))
    
        return [str(w["name"]) for w in (res.json().get("workspaces") or {}).get("workspace") or [] ]
    
    def create_workspace(self,workspace):
        """
        Return true if created; otherwise return False if already exist
        """
        try:
            data = WORKSPACE_TEMPLATE.format(workspace)
            res = self.post(self.workspaces_url(),data=data,headers=self.contenttype_header("xml"))
            logger.debug("Succeed to create the workspace({})".format(workspace))
            return True
        except requests.RequestException as ex:
            if self.has_workspace(workspace):
                return False
            else:
                raise ex
    
    def delete_workspace(self,workspace,recurse=False):
        """
        Return True if deleted; otherwise return False if doesn't exist
        """
        try:
            res = self.delete("{}?recurse={}".format(self.workspace_url(workspace),"true" if recurse else "false"))
            logger.debug("Succeed to delete the workspace({})".format(workspace))
            return True
        except ResourceNotFound as ex:
            return False
    

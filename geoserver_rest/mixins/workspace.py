import logging

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
        r = self.get(self.workspaces_url(),headers=self.accept_header("json"))
        if r.status_code >= 300:
            raise Exception("Failed to list the workspaces. code = {},message = {}".format(r.status_code, r.content))
    
        return [str(w["name"]) for w in (r.json().get("workspaces") or {}).get("workspace") or [] ]
    
    def create_workspace(self,workspace):
        """
        Return true if created; otherwise return False if already exist
        """
        data = WORKSPACE_TEMPLATE.format(workspace)
        r = self.post(self.workspaces_url(),data=data,headers=self.contenttype_header("xml"))
        if r.status_code >= 300:
            if self.has_workspace(workspace):
                return False
            else:
                raise Exception("Failed to create the workspace({}). code = {},message = {}".format(workspace,r.status_code, r.content))
    
        logger.debug("Succeed to create the workspace({})".format(workspace))
        return True
    
    def delete_workspace(self,workspace,recurse=False):
        """
        Return True if deleted; otherwise return False if doesn't exist
        """
        r = self.delete("{}?recurse={}".format(self.workspace_url(workspace),"true" if recurse else "false"))
        if r.status_code == 404:
            logger.debug("The workspace({}) doesn't exist".format(workspace))
            return False
        if r.status_code >= 300:
            raise Exception("Failed to delete the workspace({}). code = {},message = {}".format(workspace,r.status_code, r.content))
    
        logger.debug("Succeed to delete the workspace({})".format(workspace))
        return True
    

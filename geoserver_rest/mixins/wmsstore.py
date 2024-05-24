import logging

logger = logging.getLogger(__name__)

class WMSStoreMixin(object):
    def wmsstores_url(self,workspace):
        return "{0}/rest/workspaces/{1}/wmsstores".format(self.geoserver_url,workspace)
    
    def wmsstore_url(self,workspace,storename):
        return "{0}/rest/workspaces/{1}/wmsstores/{2}".format(self.geoserver_url,workspace,storename)
    
    def has_wmsstore(self,workspace,storename):
        return self.has(self.wmsstore_url(workspace,storename),headers=self.accept_header("json"))
    
    def list_wmsstores(self,workspace):
        r = self.get(self.wmsstores_url(workspace),headers=self.accept_header("json"))
        if r.status_code >= 300:
            raise Exception("Failed to list the wmsstores in workspace({}). code = {},message = {}".format(workspace,r.status_code, r.content))
    
        return [str(s["name"]) for s in (r.json().get("wmsStores") or {}).get("wmsStore") or [] ]
    
    STORE_TEMPLATE = """<?xml version="1.0" encoding="UTF-8"?>
<wmsStore>
    <name>{1}</name>
    <type>WMS</type>
    <enabled>true</enabled>
    <workspace>
        <name>{0}</name>
    </workspace>
    <metadata>
        <entry key="useConnectionPooling">true</entry>
    </metadata>
    <__default>true</__default>
    <capabilitiesURL><![CDATA[{2}]]></capabilitiesURL>
    <user>{}</user>
    <password>{}</password>
    <maxConnections>{}</maxConnections>
    <readTimeout>{}</readTimeout>
    <connectTimeout>{}</connectTimeout>
</wmsStore>
"""
    def update_wmsstore(self,workspace,storename,parameters):
        """
        update a store
        return True if created;otherwise return False if update
        """
        store_data = STORE_TEMPLATE.format(
            workspace,
            storename,
            parameters.get("capability_url"),
            parameters.get("username") or "",
            parameters.get("password") or "",
            parameters.get("max_connections") or "10",
            parameters.get("read_timeout") or "60",
            parameters.get("connect_timeout") or "30"
        )
    
        if self.has_wmsstore(workspace,storename):
            r = self.put(self.wmsstore_url(workspace,storename), headers=self.contenttype_header("xml"), data=store_data)
            created = False
        else:
            r = func(self.wmsstores_url(workspace), headers=self.contenttype_header("xml"), data=store_data)
            created = True

        if r.status_code >= 300:
            raise Exception("Failed to {} the wmsstore({}:{}). code = {} , message = {}".format("create" if created else "update",workspace,storename,r.status_code, r.content))
        
        if created:
            logger.debug("Succeed to create the wmsstore({}:{}). ".format(workspace,storename))
            return True
        else:
            logger.debug("Succeed to update the wmsstore({}:{}). ".format(workspace,storename))
            return False
            
    
    def delete_wmsstore(self,workspace,storename,recurse=False):
        """
        Return True if deleted;otherwise return False if doesn't exsit before
        """
        if not self.has_wmsstore(workspace,storename):
            logger.debug("The wmsstore({}:{}) doesn't exist".format(workspace,storename))
            return False
    
        r = self.delete("{}?recurse={}".format(self.wmsstore_url(workspace,storename),"true" if recurse else "false"))
        if r.status_code >= 300:
            raise Exception("Failed to delete wmsstore({}:{}). code = {} , message = {}".format(workspace,storename,r.status_code, r.content))
    
        logger.debug("Succeed to delete the wmsstore({}:{})".format(workspace,storename))
        return True
    

import logging

logger = logging.getLogger(__name__)

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
    <user>{3}</user>
    <password>{4}</password>
    <maxConnections>{5}</maxConnections>
    <readTimeout>{6}</readTimeout>
    <connectTimeout>{7}</connectTimeout>
</wmsStore>
"""
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
    
    def update_wmsstore(self,workspace,storename,parameters,create=None):
        """
        update a store
        return True if created;otherwise return False if update
        """
        store_data = STORE_TEMPLATE.format(
            workspace,
            storename,
            parameters.get("capabilitiesURL"),
            parameters.get("user") or "",
            parameters.get("password") or "",
            parameters.get("maxConnections") or "10",
            parameters.get("readTimeout") or "60",
            parameters.get("connectTimeout") or "30"
        )
        if create is None:
            create = False if self.has_wmsstore(workspace,storename) else True

        if create:
            r = self.post(self.wmsstores_url(workspace), headers=self.contenttype_header("xml"), data=store_data)
            if r.status_code >= 300:
                raise Exception("Failed to create the wmsstore({}:{}). code = {} , message = {}".format(workspace,storename,r.status_code, r.content))
            logger.debug("Succeed to create the wmsstore({}:{}). ".format(workspace,storename))
            return True
        else:
            r = self.put(self.wmsstore_url(workspace,storename), headers=self.contenttype_header("xml"), data=store_data)
            if r.status_code >= 300:
                raise Exception("Failed to update the wmsstore({}:{}). code = {} , message = {}".format(workspace,storename,r.status_code, r.content))
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
    

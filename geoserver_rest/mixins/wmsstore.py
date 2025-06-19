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

    def get_wmsstore(self,workspace,storename):
        res = self.get(self.wmsstore_url(workspace,storename),headers=self.accept_header("json"))
        return res.json().get("wmsStore")

    def get_wmsstore_field(self,wmsdata,field):
        """
        field:
            name:
            enabled:
            workspace
            capabilitiesURL:
            user
            maxConnections
            readTimeout
            connectTimeout

        Get the wms field from wms json data, returned by get_wmsstore
        """
        if field == "workspace":
            return wmsdata.get("workspace",{}).get("name")
        else:
            return wmsdata.get(field)
    
    def list_wmsstores(self,workspace):
        res = self.get(self.wmsstores_url(workspace),headers=self.accept_header("json"))
    
        return [str(s["name"]) for s in (res.json().get("wmsStores") or {}).get("wmsStore") or [] ]
    
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
            res = self.post(self.wmsstores_url(workspace), headers=self.contenttype_header("xml"), data=store_data)
            logger.debug("Succeed to create the wmsstore({}:{}). ".format(workspace,storename))
            return True
        else:
            res = self.put(self.wmsstore_url(workspace,storename), headers=self.contenttype_header("xml"), data=store_data)
            logger.debug("Succeed to update the wmsstore({}:{}). ".format(workspace,storename))
            return False

    def delete_wmsstore(self,workspace,storename,recurse=False):
        """
        Return True if deleted;otherwise return False if doesn't exsit before
        """
        if not self.has_wmsstore(workspace,storename):
            logger.debug("The wmsstore({}:{}) doesn't exist".format(workspace,storename))
            return False
    
        res = self.delete("{}?recurse={}".format(self.wmsstore_url(workspace,storename),"true" if recurse else "false"))
    
        logger.debug("Succeed to delete the wmsstore({}:{})".format(workspace,storename))
        return True
    

import logging

logger = logging.getLogger(__name__)

class CoverageStoreMixin(object):
    def coveragestores_url(self,workspace):
        return "{0}/rest/workspaces/{1}/coveragestores".format(self.geoserver_url,workspace)
    
    def coveragestore_url(self,workspace,storename):
        return "{0}/rest/workspaces/{1}/coveragestores/{2}".format(self.geoserver_url,workspace,storename)
    
    def has_coveragestore(self,workspace,storename):
        return self.has(self.coveragestore_url(workspace,storename),headers=self.accept_header("json"))

    def get_coveragestore(self,workspace,storename):
        """"
        Return coveragestore json data; raise ResourceNotFound if not found.
        """
        res = self.get(self.coveragestore_url(workspace,storename),headers=self.accept_header("json"))
        return res.json().get("coverageStore")

    def get_coveragestore_field(self,storedata,field):
        """
        field:
            name:
            enabled:
            workspace|namespace
        Get the coverage field from wms json data, returned by get_coveragestore
        """
        if field in ("workspace","namespace"):
            return storedata.get("workspace",{}).get("name")
        else:
            return storedata.get(field)
    
    def list_coveragestores(self,workspace):
        res = self.get(self.coveragestores_url(workspace),headers=self.accept_header("json"))
        print("%%%%%%{} = {}".format(workspace,[str(s["name"]) for s in (res.json().get("coverageStores") or {}).get("coverageStore") or [] ]))

        return [str(s["name"]) for s in (res.json().get("coverageStores") or {}).get("coverageStore") or [] ]
    

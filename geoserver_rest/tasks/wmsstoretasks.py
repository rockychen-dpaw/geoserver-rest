import json

from .base import Task

class ListWMSstores(Task):
    """
    Return [store]
    """
    arguments = ("workspace",)
    keyarguments = ("workspace",)
    category = "List WMSStores"

    def __init__(self,workspace,post_actions_factory = None):
        super().__init__(post_actions_factory = post_actions_factory)
        self.workspace = workspace

    def _format_result(self):
        return "WMSStores = {}".format(len(self.result) if self.result else 0)

    def _exec(self,geoserver):
        result = geoserver.list_wmsstores(self.workspace) or []
        result.sort()
        return result
                
class GetWMSstore(Task):
    arguments = ("workspace","wmsstore")
    keyarguments = ("workspace","wmsstore")
    category = "Get WMSStore Detail"

    def __init__(self,workspace,wmsstore,post_actions_factory = None):
        super().__init__(post_actions_factory = post_actions_factory)
        self.workspace = workspace
        self.wmsstore = wmsstore

    def _format_result(self):
        return json.dumps(self.result,indent=4) if self.result else "{}"

    def _exec(self,geoserver):
        return  geoserver.get_wmsstore(self.workspace,self.wmsstore) or {}

    def _warnings(self):
        if not self.result:
            yield (self.ERROR,"Detail is missing")
        elif not self.result["enabled"]:
            yield (self.WARNING,"WMSstore is disabled")

def createtasks_ListWMSstores(listWorkspacesTask,limit = 0):
    """
    a generator to return list datastore tasks
    """
    if not listWorkspacesTask.result:
        return
    row = 0
    for w in listWorkspacesTask.result:
        row += 1
        if limit > 0 and row > limit:
            break
        yield ListWMSstores(w,post_actions_factory=listWorkspacesTask.post_actions_factory)

    
def createtasks_GetWMSstore(listWMSStoresTask,limit = 0):
    """
    a generator to return get wmsstore tasks
    """
    if not listWMSStoresTask.result:
        return
    row = 0
    for w in listWMSStoresTask.result:
        row += 1
        if limit > 0 and row > limit:
            break
        yield GetWMSstore(listWMSStoresTask.workspace,w,post_actions_factory=listWMSStoresTask.post_actions_factory)



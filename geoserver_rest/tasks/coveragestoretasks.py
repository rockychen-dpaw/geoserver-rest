import json

from .base import Task

class ListCoverageStores(Task):
    """
    Return [store]
    """
    arguments = ("workspace",)
    keyarguments = ("workspace",)
    category = "List CoverageStores"
   
    url = None
    def __init__(self,workspace,post_actions_factory = None):
        super().__init__(post_actions_factory = post_actions_factory)
        self.workspace = workspace

    def _format_result(self):
        return """URL: {}
CoverageStores = {}""".format(self.url,len(self.result) if self.result else 0)

    def _exec(self,geoserver):
        result = geoserver.list_coveragestores(self.workspace) or []
        self.url = geoserver.coveragestores_url(self.workspace)
        result.sort()
        return result
                
class GetCoverageStore(Task):
    arguments = ("workspace","coveragestore")
    keyarguments = ("workspace","coveragestore")
    category = "Get CoverageStore Detail"

    def __init__(self,workspace,coveragestore,post_actions_factory = None):
        super().__init__(post_actions_factory = post_actions_factory)
        self.workspace = workspace
        self.coveragestore = coveragestore

    def _format_result(self):
        return json.dumps(self.result,indent=4) if self.result else "{}"

    def _exec(self,geoserver):
        return  geoserver.get_coveragestore(self.workspace,self.coveragestore) or {}

    def _warnings(self):
        if not self.result:
            yield (self.ERROR,"Detail is missing")
        elif not self.result["enabled"]:
            yield (self.WARNING,"coveragestore is disabled")

def createtasks_ListCoverageStores(listWorkspacesTask,limit = 0):
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
        yield ListCoverageStores(w,post_actions_factory=listWorkspacesTask.post_actions_factory)

    
def createtasks_GetCoverageStore(listCoverageStoresTask,limit = 0):
    """
    a generator to return get coveragestore tasks
    """
    if not listCoverageStoresTask.result:
        return
    row = 0
    for w in listCoverageStoresTask.result:
        row += 1
        if limit > 0 and row > limit:
            break
        yield GetCoverageStore(listCoverageStoresTask.workspace,w,post_actions_factory=listCoverageStoresTask.post_actions_factory)



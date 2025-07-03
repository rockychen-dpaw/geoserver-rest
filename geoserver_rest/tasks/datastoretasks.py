import json

from .base import Task

class ListDatastores(Task):
    """
    Return list of datastore
    """
    arguments = ("workspace",)
    keyarguments = ("workspace",)
    category = "List Datastores"

    def __init__(self,workspace,post_actions_factory = None):
        super().__init__(post_actions_factory = post_actions_factory)
        self.workspace = workspace

    def _format_result(self):
        return "Datastores = {}".format(len(self.result) if self.result else 0)

    def _exec(self,geoserver):
        result =  geoserver.list_datastores(self.workspace) or []
        result.sort()
        return result

class GetDatastore(Task):
    arguments = ("workspace","datastore")
    keyarguments = ("workspace","datastore")
    category = "Get Datastore Detail"

    def __init__(self,workspace,datastore,post_actions_factory = None):
        super().__init__(post_actions_factory = post_actions_factory)
        self.workspace = workspace
        self.datastore = datastore

    def _format_result(self):
        return json.dumps(self.result,indent=4) if self.result else "{}"

    def _exec(self,geoserver):
        return  geoserver.get_datastore(self.workspace,self.datastore) or {}

    def _warnings(self):
        if not self.result:
            yield (self.ERROR,"Detail is missing")
        elif not self.result["enabled"]:
            yield (self.WARNING,"Datastore is disabled")

def createtasks_ListDatastores(listWorkspacesTask,limit = 0):
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
        yield ListDatastores(w,post_actions_factory=listWorkspacesTask.post_actions_factory)

def createtasks_GetDatastore(listDatastoresTask,limit = 0):
    """
    a generator to return get datastore tasks
    """
    if not listDatastoresTask.result:
        return
    row = 0
    for datastore in listDatastoresTask.result:
        row += 1
        if limit > 0 and row > limit:
            break
        yield GetDatastore(listDatastoresTask.workspace,datastore,post_actions_factory=listDatastoresTask.post_actions_factory)



from .base import Task

class ListDatastores(Task):
    """
    Return [store]
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

    
    
                


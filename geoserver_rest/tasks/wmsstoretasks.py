from .base import Task

class ListWMSstores(Task):
    """
    Return [store]
    """
    arguments = ("workspace",)
    category = "List WMSstores"

    def __init__(self,workspace,post_actions_factory = None):
        super().__init__(post_actions_factory = post_actions_factory)
        self.workspace = workspace

    def _format_result(self):
        return " , ".join(self.result) if self.result else ""

    def _exec(self,geoserver):
        return geoserver.list_wmsstores(self.workspace) or []
                
def createtasks_ListWMSstores(listWorkspacesTask):
    """
    a generator to return list datastore tasks
    """
    if not listWorkspacesTask.result:
        return
    for w in listWorkspacesTask.result:
        yield ListWMSstores(w,post_actions_factory=listWorkspacesTask.post_actions_factory)

    
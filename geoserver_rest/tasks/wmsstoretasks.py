from .base import Task

class ListWMSstores(Task):
    """
    Return [(workspace,[stores])]
    """
    arguments = ("workspace",)
    category = "List WMSstores"
    workspace = None

    def __init__(self,workspace=None,post_actions_factory = None):
        super().__init__(post_actions_factory = post_actions_factory)
        if workspace:
            self.workspace = workspace

    def _format_result(self):
        if self.workspace:
            return "WMSstores : {}".format(len(self.result[0][1]))
        else:
            return "\r\n".join("Workspace : {} , WMSstores : {}".format(w,len(stores)) for w,stores in self.result)

    def _exec(self,geoserver):
        if self.workspace:
            return [(self.workspace, geoserver.list_wmsstores(self.workspace))]
        else:
            result = []
            for w in geoserver.list_workspaces():
                result.append((w,geoserver.list_wmsstores(w)))
            return result
                
def createtasks_ListWMSstores(listWorkspacesTask):
    """
    a generator to return list datastore tasks
    """
    if not listWorkspacesTask.result:
        return
    for w in listWorkspacesTask.result:
        yield ListWMSstores(w,post_actions_factory=listWorkspacesTask.post_actions_factory)

    

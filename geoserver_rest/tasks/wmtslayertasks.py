from .base import Task

class ListWMTSLayers(Task):
    """
    Return [(workspace,[wmtslayer])]
    """
    arguments = ("workspace",)
    category = "List WMTSLayers"
    workspace = None
    wmsstore = None
    def __init__(self,workspace=None,post_actions_factory = None):
        super().__init__(post_actions_factory = post_actions_factory) 
        if workspace:
            self.workspace = workspace

    def _format_result(self):
        return "\r\n".join("Workspace : {} , WMTSLayers : {}".format(w,len(layers)) for w,layers in self.result)

    def _exec(self,geoserver):
        if self.workspace:
            return [(self.workspace,geoserver.list_gwclayers(self.workspace))]
        else:
            result = []
            for workspace in geoserver.list_workspaces():
                result.append((workspace,geoserver.list_gwclayers(workspace)))
            return result
        
def createtasks_ListWMTSLayers(listWorkspacesTask):
    """
    a generator to return list datastore tasks
    """
    if not listWorkspacesTask.result:
        return
    for w in listWorkspacesTask.result:
        yield ListWMTSLayers(w,post_actions_factory=listWorkspacesTask.post_actions_factory)

    

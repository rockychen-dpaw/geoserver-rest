import json

from .base import Task
from .. import settings

class ListWorkspaces(Task):
    """
    Return [workspaces]
    """
    category = "List Workspaces"
    def __init__(self,post_actions_factory = None):
        super().__init__(post_actions_factory = post_actions_factory)

    def _format_result(self):
        return "Workspaces = {}".format(len(self.result) if self.result else 0)

    def _exec(self,geoserver):
        return geoserver.list_workspaces()

    
class ListResourcesInWorkspace(Task):
    """
    Return ([datastore],[wmsstore],[layergroup])
    """
    arguments = ("workspace",)
    keyarguments = ("workspace",)
    category = "List Resources In Workspace"

    def __init__(self,workspace,post_actions_factory = None):
        super().__init__(post_actions_factory = post_actions_factory)
        self.workspace = workspace

    def _format_result(self):
        return "Datastores = {} , WMSStores = {} , Layergroups = {}".format(
            len(self.result[0]) if self.result[0] else 0,
            len(self.result[1]) if self.result[1] else 0,
            len(self.result[2]) if self.result[2] else 0
        )

    def _warnings(self):
        if settings.IGNORE_EMPTY_WORKSPACE:
            return
        if not self.result or all((not data) for data in self.result):
            yield (self.WARNING,"The workspace({}) is empty.".format(self.workspace))

    def _exec(self,geoserver):
        return (
            geoserver.list_datastores(self.workspace) or [],
            geoserver.list_wmsstores(self.workspace) or [],
            geoserver.list_layergroups(self.workspace) or []
        )

        
def createtasks_ListWorkspaces(task,limit = 0):
    """
    a generator to return list datastore tasks
    """
    if not task.is_succeed:
        return
    yield ListWorkspaces(post_actions_factory=task.post_actions_factory)

def createtasks_ListResourcesInWorkspace(listWorkspacesTask,limit = 0):
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
        yield ListResourcesInWorkspace(w,post_actions_factory=listWorkspacesTask.post_actions_factory)


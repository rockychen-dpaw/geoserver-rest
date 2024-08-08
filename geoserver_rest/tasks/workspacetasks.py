import json

from .base import Task

class ListWorkspaces(Task):
    """
    Return [workspaces]
    """
    category = "List Workspaces"
    def __init__(self,post_actions_factory = None):
        super().__init__(post_actions_factory = post_actions_factory)

    def _format_result(self):
        return "workspaces : {}\r\n{}".format(len(self.result),json.dumps(self.result) if self.result else "")

    def _exec(self,geoserver):
        return geoserver.list_workspaces()

    
        

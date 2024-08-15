import json

from .base import Task
from .. import timezone
from .. import settings
from .wmsstoretasks import ListWMSStores
from .workspacetasks import ListResourcesInWorkspace

class ListLayergroups(Task):
    """
    Return [layergroup]
    """
    arguments = ("workspace",)
    category = "List Layergroups"
    def __init__(self,workspace,post_actions_factory = None):
        super().__init__(post_actions_factory = post_actions_factory) 
        self.workspace = workspace

    def _format_result(self):
        return "Layergroups : {}".format(len(self.result) if self.result else 0) 

    def _exec(self,geoserver):
        result = geoserver.list_layergroups(self.workspace)
        result.sort()
        return result
        
class GetLayergroupDetail(Task):
    """
    Return a dict of layer group detail
    """
    arguments = ("workspace","layergroup")
    category = "Get Layergroup Detail "

    def __init__(self,workspace,layergroup,post_actions_factory = None):
        super().__init__(post_actions_factory = post_actions_factory) 
        self.workspace = workspace
        self.layergroup = layergroup

    def _format_result(self):
        return json.dumps(self.result,indent=4) if self.result else "{}"

    def _warnings(self):
        if not self.result:
            yield (self.ERROR,"Detail is missing")
        msg = None
        level = self.WARNING
        if self.result.get("gwc"):
            for gridset in settings.GWC_GRIDSETS:
                if not any(gridsetdata["gridSetName"] == gridset  for gridsetdata in self.result["gwc"]["gridSubsets"]):
                    msg = "{}{}{}".format(msg or "", "\r\n" if msg else "","The gridset({}) was not configured".format(gridset))
            if not self.result["gwc"]["enabled"]:
                msg = "{}{}{}".format(msg or "", "\r\n" if msg else "","The GWC was disabled.")
            if self.result["gwc"].get("expireCache",0) < 0:
                msg = "{}{}{}".format(msg or "", "\r\n" if msg else "","The GWC server cache was disabled.")
                level = self.ERROR
            if self.result["gwc"].get("expireClients",0) > settings.MAX_EXPIRE_CLIENTS:
                msg = "{}{}{}".format(msg or "", "\r\n" if msg else "","The GWC client cache is greater than {} seconds".format(settings.MAX_EXPIRE_CLIENTS_STR))

        if msg:
            yield (level,msg)

    def _exec(self,geoserver):
        result = {}
        #get the layer detail
        detail = geoserver.get_layergroup(self.workspace,self.layergroup)
        for k in ["title","publishables","bounds"]:
            if not detail.get(k):
                continue
            result[k] = detail[k]
        detail = geoserver.get_gwclayer(self.workspace,self.layergroup)
        if detail:
            result["gwc"] = {}
            for k in ["expireClients","expireCache","gridSubsets","enabled"]:
                result["gwc"][k] = detail[k]
        
        return result

def createtasks_ListLayergroups(listWorkspacesTask,limit = 0):
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
        yield ListLayergroups(w,post_actions_factory=listWorkspacesTask.post_actions_factory)

    
def createtasks_GetLayergroupDetail(task,limit = 0):
    """
    a generator to return layergroup detail tasks
    """
    if isinstance(task,ListLayergroups):
        result = task.result
    elif isinstance(task,ListResourcesInWorkspace):
        result = (task.result[2] or []) if task.result else []
    else:
        return

    if not result:
        return

    row = 0
    for layergroup in result:
        row += 1
        if limit > 0 and row > limit:
            break
        yield GetLayergroupDetail(task.workspace,layergroup,post_actions_factory=task.post_actions_factory)


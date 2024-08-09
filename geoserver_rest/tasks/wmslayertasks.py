from .base import Task
from .. import timezone

class ListWMSLayers(Task):
    """
    Return [(workspace,[(wmsstore,[wmslayer])])]
    """
    arguments = ("workspace","wmsstore")
    category = "List WMSLayers"
    workspace = None
    wmsstore = None
    def __init__(self,workspace=None,wmsstore=None,post_actions_factory = None):
        super().__init__(post_actions_factory = post_actions_factory) 
        if workspace:
            self.workspace = workspace
            if wmsstore:
                self.wmsstore = wmsstore

    def _format_result(self):
        return "\r\n".join("Workspace : {}\r\n{}".format(
            w,
            "\r\n".join("    WMSStore : {} , WMSLayers : {}".format(s,len(wmslayers)) for s,wmslayers in storedata)
        ) for w,storedata in self.result if storedata)

    def _warnings(self):
        for workspace,storedatas in self.result:
            for store,layers in storedatas:
                if not layers:
                    yield (self.category,
                        "Warning",
                        timezone.format(self.starttime,"%Y-%m-%d %H:%M:%S.%f") if self.starttime else "",
                        timezone.format(self.endtime,"%Y-%m-%d %H:%M:%S.%f") if self.endtime else "",
                        "The wmsstore({}) is empty.".format(store)
                    )
    def _exec(self,geoserver):
        if self.workspace:
            if self.wmsstore:
                return [(self.workspace,[(self.wmsstore,geoserver.list_wmslayers(self.workspace,self.wmsstore))])]
            else:
                result = [(self.workspace,[])]
                for store in geoserver.list_wmsstores(self.workspace):
                    result[0][1].append((store,geoserver.list_featuretypes(self.workspace,store)))
                return result
        else:
            result = []
            for workspace in geoserver.list_workspaces():
                result.append((workspace,[]))
                for store in geoserver.list_wmsstores(workspace):
                    result[-1][1].append((store,geoserver.list_featuretypes(workspace,store)))

            return result
        
def createtasks_ListWMSLayers(listWMSstoresTask):
    """
    a generator to return featuretypes tasks
    """
    if not listWMSstoresTask.result:
        return
    for w,stores in listWMSstoresTask.result:
        for store in stores:
            yield ListWMSLayers(w,store,post_actions_factory=listWMSstoresTask.post_actions_factory)



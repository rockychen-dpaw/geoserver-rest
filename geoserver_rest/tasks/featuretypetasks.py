from .base import Task

class ListFeatureTypes(Task):
    """
    Return [(workspace,[(store,[featuretype]))]
    """
    arguments = ("workspace","datastore")
    category = "List Features"
    workspace = None
    datastore = None
    def __init__(self,workspace=None,datastore=None,post_actions_factory = None):
        super().__init__(post_actions_factory = post_actions_factory) 
        if workspace:
            self.workspace = workspace
            if datastore:
                self.datastore = datastore

    def _format_result(self):
        return "\r\n".join("Workspace : {}\r\n{}".format(w,"\r\n".join("    Datastore : {} , Featuretypes : {}".format(store,len(featuretypes)) for store,featuretypes in storedatas)) for w,storedatas in self.result if storedatas)

    def _exec(self,geoserver):
        if self.workspace:
            if self.datastore:
                return [(self.workspace,[(self.datastore,geoserver.list_featuretypes(self.workspace,storename=self.datastore))])]
            else:
                result = [(self.workspace,[])]
                for store in geoserver.list_datastores(self.workspace):
                    result[0][1].append((store,geoserver.list_featuretypes(self.workspace,storename=store)))
                return result
        else:
            result = []
            for workspace in geoserver.list_workspaces():
                result.append((workspace,[]))
                for store in geoserver.list_datastores(workspace):
                    result[-1][1].append((store,geoserver.list_featuretypes(workspace,storename=store)))
            return result

    def _warnings(self):
        for workspace,storedatas in self.result:
            for store,layers in storedatas:
                if not layers:
                    yield (self.key,
                        "Warning",
                        timezone.format(self.starttime,"%Y-%m-%d %H:%M:%S.%f") if self.starttime else "",
                        timezone.format(self.endtime,"%Y-%m-%d %H:%M:%S.%f") if self.endtime else "",
                        "The datastore({}) is empty, can be deleted.".format(store)
                    )

class GetFeatureTypeStyles(Task):
    """
    Return [(workspace,[(store,[(featuretype,((workspace,default style name),[(workspace,alternative style name)]))])])]
    """
    arguments = ("workspace","datastore","featuretype")
    workspace = None
    datastore = None
    featuretype = None
    category = "Get styles of featuretypes"

    def __init__(self,workspace=None,datastore=None,featuretype=None,post_actions_factory = None):
        super().__init__(post_actions_factory = post_actions_factory) 
        if workspace:
            self.workspace = workspace
            if datastore:
                self.datastore = datastore
                if featuretype:
                    self.featuretype = featuretype

    def _format_result(self):
        msg = "\r\n".join("Workspace : {}\r\n{}".format(
            w,
            "\r\n".join("    Datastore : {} , Featuretypes : {}".format(s,len(featuretypes)) for s,featuretypes in storedata)
        ) for w,storedata in self.result )

        return "Get default and alternative styles for featuretypes.\r\n{}".format(msg)

    def _exec(self,geoserver):
        if self.workspace:
            if self.datastore:
                if self.featuretype:
                    return [(self.workspace,[(self.datastore,[(self.featuretype,geoserver.get_layer_styles(self.workspace,self.featuretype))])])]
                else:
                    result = [(self.workspace,[(self.datastore,[])])]
                    for featuretype in geoserver.list_featuretypes(self.workspace,self.datastore):
                        result[0][1][0][1].append((featuretype,geoserver.get_layer_styles(self.workspace,featuretype)))
                    return result
            else:
                result = [(self.workspace,[])]
                for store in geoserver.list_datastores(self.workspace):
                    result[0][1].append((store,[]))
                    for featuretype in geoserver.list_featuretypes(self.workspace,store):
                        result[0][1][-1][1].append((featuretype,geoserver.get_layer_styles(self.workspace,featuretype)))
                return result
        else:
            result = []
            for workspace in geoserver.list_workspaces():
                result.append((workspace,[]))
                for store in geoserver.list_datastores(self.workspace):
                    result[-1][1].append((store,[]))
                    for featuretype in geoserver.list_featuretypes(self.workspace,store):
                        result[-1][1][-1][1].append((featuretype,geoserver.get_layer_styles(self.workspace,featuretype)))

            return result
        
def createtasks_ListFeatureTypes(listDatastoresTask):
    """
    a generator to return featuretypes tasks
    """
    if not listDatastoresTask.result:
        return
    for w,stores in listDatastoresTask.result:
        for store in stores:
            yield ListFeatureTypes(w,store,post_actions_factory=listDatastoresTask.post_actions_factory)


def createtasks_GetFeatureTypeStyles(listFeatureTypesTask):
    """
    a generator to return featuretype styles tasks
    """
    if not listFeatureTypesTask.result:
        return
    for w,storesdata in listFeatureTypesTask.result:
        for store,featuretypes in storesdata:
            for featuretype in featuretypes:
                yield GetFeatureTypeStyles(w,store,featuretype,post_actions_factory=listFeatureTypesTask.post_actions_factory)



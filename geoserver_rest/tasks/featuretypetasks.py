from .base import Task
from .. import timezone

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
        if self.workspace:
            if self.datastore:
                return "Featuretypes : {}".format(len(self.result[0][1][0][1]))
            else:
                return "\r\n".join("Datastore : {} , Featuretypes : {}".format(store,len(featuretypes)) for store,featuretypes in self.result[0][1])
        else:
            return "\r\n".join("Workspace : {}\r\n{}".format(w,"\r\n".join("    Datastore : {} , Featuretypes : {}".format(store,len(featuretypes)) for store,featuretypes in storedatas)) for w,storedatas in self.result)

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
                    yield "The datastore({}) is empty.".format(store)

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

    @staticmethod
    def get_layers_has_customizedstyle(featuretypesdata):
        layers_has_customized_defaultstyle = 0
        layers_has_customized_alternativestyle = 0
        for featuretype,stylesdata in featuretypesdata:
            defaultstyle,alternativestyles = stylesdata
            if defaultstyle[0] != None or defaultstyle[1] not in ("generic","line","point","polygon","raster"):
                layers_has_customized_defaultstyle += 1
            for style in alternativestyles:
                if style[0] != None or style[1] not in ("generic","line","point","polygon","raster"):
                    layers_has_customized_alternativestyle += 1
                    break
        return (layers_has_customized_defaultstyle,layers_has_customized_alternativestyle)

    def _format_result(self):
        if self.workspace:
            if self.datastore:
                if self.featuretype:
                    featuretypedata = self.result[0][1][0][1][0]
                    msg = "Featuretypes : {} , Default Style : {} ,  Alternative Styles : [{}] ".format(
                              featuretypedata[0],
                              "{}:{}".format(*featuretypedata[1][0]) if featuretypedata[1][0][0] else featuretypedata[1][0][1],
                              ", ".join(("{}:{}".format(*style) if style[0] else style[1]) for style in (featuretypedata[1][1] or []))
                           )
                else:
                    storedata = self.result[0][1][0]
                    msg = "Featuretypes : {} , layers has customized default style : {} , layers has customized alternative style : {}".format(
                            len(storedata[1]),
                            *self.get_layers_has_customizedstyle(storedata[1])
                        )
            else:
                msg = "\r\n".join("Datastore : {} , Featuretypes : {} , layers has customized default style : {} , layers has customized alternative style : {}".format(
                        s,
                        len(featuretypesdata),
                        *self.get_layers_has_customizedstyle(featuretypesdata)) for s,featuretypesdata in (self.result[0][1] or [])
                    )
        else:
            msg = "\r\n".join("Workspace : {}\r\n{}".format(
                w,
                "\r\n".join("    Datastore : {} , Featuretypes : {} , layers has customized default style : {} , layers has customized alternative style : {}".format(
                    s,
                    len(featuretypesdata),
                    *self.get_layers_has_customizedstyle(featuretypesdata)) for s,featuretypesdata in storesdata
                )
            ) for w,storesdata in self.result )

        return msg

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



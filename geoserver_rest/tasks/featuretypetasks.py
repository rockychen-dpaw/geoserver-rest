import json

from .base import Task
from .. import timezone

class ListFeatureTypes(Task):
    """
    Return [featuretype]
    """
    arguments = ("workspace","datastore")
    category = "List Features"
    def __init__(self,workspace,datastore,post_actions_factory = None):
        super().__init__(post_actions_factory = post_actions_factory) 
        self.workspace = workspace
        self.datastore = datastore

    def _format_result(self):
        return "FeatureTypes = {}".format(len(self.result) if self.result else 0) 

    def _exec(self,geoserver):
        return geoserver.list_featuretypes(self.workspace,storename=self.datastore) or []

    def _warnings(self):
        if not self.result:
            yield "The datastore({}:{}) is empty.".format(self.workspace,self.datastore)

class GetFeatureTypeDetail(Task):
    """
    Return a dict of feature type detail
    """
    arguments = ("workspace","datastore","featuretype")
    category = "Get Featuretype Detail "

    def __init__(self,workspace,datastore,featuretype,post_actions_factory = None):
        super().__init__(post_actions_factory = post_actions_factory) 
        self.workspace = workspace
        self.datastore = datastore
        self.featuretype = featuretype

    def _format_result(self):
        return json.dumps(self.result,indent=4) if self.result else "{}"

    def _exec(self,geoserver):
        result = {}
        #get the feature details
        detail = geoserver.get_featuretype(self.workspace,self.featuretype)
        for k in ["nativeName","title","abstract","srs","nativeBoundingBox","latLonBoundingBox","enabled","attributes"]:
            if not detail.get(k):
                continue
            if k == "attributes":
                result[k] = []
                for attr in detail[k]["attribute"]:
                    result[k].append({})
                    for n in ["name","nillable","binding"]:
                        result[k][-1][n] = attr[n]
                continue
            result[k] = detail[k]
        #get the feature styles
        styles = geoserver.get_layer_styles(self.workspace,self.featuretype)
        result["defaultStyle"] = (":".join(styles[0]) if styles[0][0] else styles[0][1]) if styles else None
        result["alternativeStyles"] = [("{}:{}".format(w,style) if w else style)  for w,style in styles[1]] if styles and styles[1] else []
        #get the gwc details
        detail = geoserver.get_gwclayer(self.workspace,self.featuretype)
        if detail:
            result["gwc"] = {}
            for k in ["expireClients","expireCache","gridSubsets","enabled"]:
                result["gwc"][k] = detail[k]
        
        return result

class GetFeatureCount(Task):
    """
    Return a dict of feature type detail
    """
    arguments = ("workspace","datastore","featuretype")
    category = "Get Featuretype Detail "

    def __init__(self,workspace,datastore,featuretype,post_actions_factory = None):
        super().__init__(post_actions_factory = post_actions_factory) 
        self.workspace = workspace
        self.datastore = datastore
        self.featuretype = featuretype

    def _format_result(self):
        return "Features = {}".format(self.result if self.result else 0)

    def _exec(self,geoserver):
        return geoserver.get_featurecount(self.workspace,self.featuretype)

def createtasks_ListFeatureTypes(listDatastoresTask):
    """
    a generator to return featuretypes tasks
    """
    if not listDatastoresTask.result:
        return
    for store in listDatastoresTask.result:
        yield ListFeatureTypes(listDatastoresTask.workspace,store,post_actions_factory=listDatastoresTask.post_actions_factory)


def createtasks_GetFeatureTypeDetail(listFeatureTypesTask):
    """
    a generator to return featuretype styles tasks
    """
    if not listFeatureTypesTask.result:
        return
    for featuretype in listFeatureTypesTask.result:
        yield GetFeatureTypeDetail(listFeatureTypesTask.workspace,listFeatureTypesTask.datastore,featuretype,post_actions_factory=listFeatureTypesTask.post_actions_factory)


def createtasks_GetFeatureCount(listFeatureTypesTask):
    """
    a generator to return featuretype styles tasks
    """
    if not listFeatureTypesTask.result:
        return
    for featuretype in listFeatureTypesTask.result:
        yield GetFeatureCount(listFeatureTypesTask.workspace,listFeatureTypesTask.datastore,featuretype,post_actions_factory=listFeatureTypesTask.post_actions_factory)



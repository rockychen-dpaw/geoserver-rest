import os
import psycopg
import re
from .tasks import Task
from .geoserver import Geoserver
from . import settings

styleid_re = re.compile("\\<id\\>(?P<id>StyleInfo[^\\<\\>]+)\\</id\\>",re.I)
workspaceid_re = re.compile("\\<id\\>(?P<id>WorkspaceInfo[^\\<\\>]+)\\</id\\>",re.I)
name_re = re.compile("\\<name\\>(?P<name>[^\\<\\>]+)\\</name\\>",re.I)
namespacename_re = re.compile("\\<prefix\\>(?P<name>[^\\<\\>]+)\\</prefix\\>",re.I)
namespaceid_re = re.compile("\\<id\\>(?P<id>NamespaceInfo[^\\<\\>]+)\\</id\\>",re.I)
datastoreid_re = re.compile("\\<id\\>(?P<id>DataStoreInfo[^\\<\\>]+)\\</id\\>",re.I)
wmsstoreid_re = re.compile("\\<id\\>(?P<id>WMSStoreInfo[^\\<\\>]+)\\</id\\>",re.I)
coveragestoreid_re = re.compile("\\<id\\>(?P<id>CoverageStoreInfo[^\\<\\>]+)\\</id\\>",re.I)
featuretypeid_re = re.compile("\\<id\\>(?P<id>FeatureTypeInfo[^\\<\\>]+)\\</id\\>",re.I)
layerid_re = re.compile("\\<id\\>(?P<id>LayerInfo[^\\<\\>]+)\\</id\\>",re.I)
wmslayerid_re = re.compile("\\<id\\>(?P<id>WMSLayerInfo[^\\<\\>]+)\\</id\\>",re.I)
coveragelayerid_re = re.compile("\\<id\\>(?P<id>CoverageInfo[^\\<\\>]+)\\</id\\>",re.I)
layergroupid_re = re.compile("\\<id\\>(?P<id>LayerGroupInfo[^\\<\\>]+)\\</id\\>",re.I)
gwclayerid_re = re.compile("\\<id\\>(?P<id>[^\\<\\>]+)\\</id\\>",re.I)
gwclayername_re = re.compile("\\<name\\>(?P<name>[^\\<\\>]+)\\</name\\>")

class GeoserverDataConsistencyCheck(object):
    def __init__(self,data_dir=None):
        self.geoserver_data_dir = data_dir or os.environ.get("GEOSERVER_DATA_DIR")
        self.styleids = {}
        self.styles = {}
    
        self.workspaceids =  {}
        self.namespaceids = {}
    
        self.datastoreids = {}
    
        self.wmsstoreids = {}
    
        self.coveragestoreids = {}
    
        self.layerids = {}
        self.layers = {}
    
        self.featuretypeids = {}
        self.featuretypes = {}
    
        self.wmslayerids = {}
        self.wmslayers = {}
    
        self.coveragelayerids = {}
        self.coveragelayers = {}
    
        self.layergroupids = {}
        self.layergroups = {}
    
        self.gwclayers = {}

        self.errors = []
        self.warnings = []

        self.cleaned_datas = []


    @property
    def enabled(self):
        return self.geoserver_data_dir and os.path.exists(self.geoserver_data_dir) and os.path.isdir(self.geoserver_data_dir)

    def _workspaces_xml(self):
        """
        A generator to return (workspacelocation,xml data)
        """
        workspacespath = os.path.join(self.geoserver_data_dir,"workspaces")
        if not os.path.exists(workspacespath):
            return
        
        for workspace in os.listdir(workspacespath):
            if workspace == "styles":
                continue
            elif not os.path.isdir(os.path.join(workspacespath,workspace)):
                #not a workspace
                continue
            else:
                workspacepath = os.path.join(workspacespath,workspace)
                if not os.path.exists(os.path.join(workspacepath,"workspace.xml")):
                    #not a workspace
                    continue

                workspacefile = os.path.join(workspacepath,"workspace.xml")
                with open(workspacefile) as f:
                    yield (workspacefile,f.read())


    def _load_workspaces(self,geoserver):
        for workspacelocation,workspacedata in self._workspaces_xml():
            m = name_re.search(workspacedata)
            if not m:
                self.errors.append((workspacelocation,"Can't find workspace name in the workspace xml({})".format(workspacelocation)))
                workspace = ""
            else:
                workspace = m.group("name")

            m = workspaceid_re.search(workspacedata)
            if not m:
                self.errors.append(("{}({})".format(workspace,workspacelocation),"Can't find workspace id in the workspace({}({}))".format(workspace,workspacelocation)))
                continue
            workspaceid = m.group("id")
            self.workspaceids[workspaceid] = (workspace,workspacelocation)

        print("Load {} workspaces".format(len(self.workspaceids)))

    def _namespaces_xml(self):
        """
        A generator to return (namespaceloctation,namespace xml data)
        """
        namespacespath = os.path.join(self.geoserver_data_dir,"workspaces")
        if not os.path.exists(namespacespath):
            return
        
        for namespace in os.listdir(namespacespath):
            if namespace == "styles":
                continue
            elif not os.path.isdir(os.path.join(namespacespath,namespace)):
                #not a workspace
                continue
            else:
                namespacepath = os.path.join(namespacespath,namespace)
                if not os.path.exists(os.path.join(namespacepath,"namespace.xml")) :
                    #not a namespace
                    continue

                namespacefile = os.path.join(namespacepath,"namespace.xml")
                with open(namespacefile) as f:
                    yield (namespacefile, f.read())


    def _load_namespaces(self,geoserver):
        for namespacelocation,namespacedata in self._namespaces_xml():
            m = namespacename_re.search(namespacedata)
            if not m:
                self.errors.append((namespacelocation,"Can't find namesoace name in the namespace xm ({})".format(namespacelocation)))
                namespace = ""
            else:
                namespace = m.group("name")

            m = namespaceid_re.search(namespacedata)
            if not m:
                self.errors.append(("{}({})".format(namespace,namespacelocation),"Can't find namespace id in the namespace({}({}))".format(namespace,namespacelocation)))
                continue

            namespaceid = m.group("id")
            if not next(((workspaceid,data) for workspaceid,data in self.workspaceids.items() if data[0] == namespace),None):
                self.errors.append((namespace,"The workspace assoicated with the namespace({}) doesn't exist".format(namespace)))

            self.namespaceids[namespaceid] = (namespace,namespacelocation)

        for workspace,workspacelocation in self.workspaceids.values():
            if not next(((namespaceid,data) for namespaceid,data in self.namespaceids.items() if data[0] == workspace),None):
                self.errors.append((workspace,"The namespace associated with the workspace({}) doesn't exist".format(workpace)))

        print("Load {} namespaces".format(len(self.namespaceids)))

    def _datastores_xml(self):
        """
        A generator to return (storelocation,xmldata)
        """
        workspacespath = os.path.join(self.geoserver_data_dir,"workspaces")
        if not os.path.exists(workspacespath):
            return
 
        for workspace in os.listdir(workspacespath):
            if workspace == "styles":
                continue
            elif not os.path.isdir(os.path.join(workspacespath,workspace)):
                #not a workspace
                continue
            else:
                workspacepath = os.path.join(workspacespath,workspace)
                for store in os.listdir(os.path.join(workspacepath)):
                    if not os.path.isdir(os.path.join(workspacepath,store)):
                        continue
                    storefile = os.path.join(workspacepath,store,"datastore.xml")
                    if not os.path.exists(storefile):
                        #has no datastore
                        continue

                    with open(storefile) as f:
                        yield (storefile,f.read())

    def _load_datastores(self,geoserver):
        previous_workspace = None
        count = 0
        for storelocation,data in self._datastores_xml():
            m = name_re.search(data)
            if not m:
                self.errors.append((storelocation,"Can't find datastore name in the datastore xml({})".format(storelocation)))
                continue

            store = m.group("name")

            m = workspaceid_re.search(data)
            if not m:
                self.errors.append(("{}({})".format(store,storelocation),"Can't find workspace id in the datastore({}({})) .".format(store,storelocation)))
                continue

            workspaceid = m.group("id")
            if workspaceid not in self.workspaceids:
                self.errors.append(("{}({})".format(store,storelocation),"The workspace id '{}' to which the datastore({}({})) belongs doesn't exist".format(workspaceid,store,storelocation)))
                continue

            workspace = self.workspaceids[workspaceid][0]

            if previous_workspace != workspace:
                if previous_workspace and count > 0:
                    print("Load {1} datastores in workspace '{0}'".format(previous_workspace,count))

                previous_workspace = workspace
                count = 0

            m = datastoreid_re.search(data)
            if not m:
                self.errors.append(("{}:{}".format(workspace,store),"Can't find datastore id in the datastore({}:{}({}))".format(workspace,store,storelocation)))
                continue

            count += 1
            storeid = m.group("id")
            self.datastoreids[storeid] = (workspace,store,storelocation)

        if previous_workspace and count > 0:
            print("Load {1} datastores in workspace '{0}'".format(previous_workspace,count))

        print("Load {} datastores".format(len(self.datastoreids)))

    def _wmsstores_xml(self):
        """
        A generator to return (storelocation,xmldata)
        """
        workspacespath = os.path.join(self.geoserver_data_dir,"workspaces")
        if not os.path.exists(workspacespath):
            return

        for workspace in os.listdir(workspacespath):
            if workspace == "styles":
                continue
            elif not os.path.isdir(os.path.join(workspacespath,workspace)):
                #not a workspace
                continue
            else:
                workspacepath = os.path.join(workspacespath,workspace)
                for store in os.listdir(os.path.join(workspacepath)):
                    if not os.path.isdir(os.path.join(workspacepath,store)):
                        continue
                    storefile = os.path.join(workspacepath,store,"wmsstore.xml")
                    if not os.path.exists(storefile):
                        #has no wmsstore
                        continue

                    with open(storefile) as f:
                        yield (storefile,f.read())

    def _load_wmsstores(self,geoserver):
        previous_workspace = None
        count = 0
        for storelocation,data in self._wmsstores_xml():
            m = name_re.search(data)
            if not m:
                self.errors.append((storelocation,"Can't find wmsstore name in the wmstore xml({})".format(storelocation)))
                continue

            store = m.group("name")

            m = workspaceid_re.search(data)
            if not m:
                self.errors.append(("{}({})".format(store,storelocation),"Can't find workspace id in the wmsstore({}({})) .".format(store,storelocation)))
                continue

            workspaceid = m.group("id")
            if workspaceid not in self.workspaceids:
                self.errors.append(("{}({})".format(store,storelocation),"The workspace id '{}' to which the wmsstore({}({})) belongs doesn't exist".format(workspaceid,store,storelocation)))
                continue

            workspace = self.workspaceids[workspaceid][0]

            if previous_workspace != workspace:
                if previous_workspace and count > 0:
                    print("Load {1} wmsstores in workspace '{0}'".format(previous_workspace,count))

                previous_worksapce = workspace
                count = 0

            m = wmsstoreid_re.search(data)
            if not m:
                self.errors.append(("{}:{}".format(workspace,store),"Can't find wmsstore id in wmsstore({}:{}({}))".format(workspace,store,storelocation)))
                continue

            count += 1
            storeid = m.group("id")
            self.wmsstoreids[storeid] = (workspace,store,storelocation)

        if previous_workspace and count > 0:
            print("Load {1} wmsstores in workspace '{0}'".format(workspace,count))

        print("Load {} wmsstores".format(len(self.wmsstoreids)))

    def _coveragestores_xml(self):
        """
        A generator to return (storelocation,xmldata)
        """
        workspacespath = os.path.join(self.geoserver_data_dir,"workspaces")
        if not os.path.exists(workspacespath):
            return

        for workspace in os.listdir(workspacespath):
            if workspace == "styles":
                continue
            elif not os.path.isdir(os.path.join(workspacespath,workspace)):
                #not a workspace
                continue
            else:
                workspacepath = os.path.join(workspacespath,workspace)
                for store in os.listdir(os.path.join(workspacepath)):
                    if not os.path.isdir(os.path.join(workspacepath,store)):
                        continue
                    storefile = os.path.join(workspacepath,store,"coveragestore.xml")
                    if not os.path.exists(storefile):
                        #has no coveragestore
                        continue

                    with open(storefile) as f:
                        yield (storefile,f.read())

    def _load_coveragestores(self,geoserver):
        previous_workspace = None
        count = 0
        for storelocation,data in self._coveragestores_xml():
            m = name_re.search(data)
            if not m:
                self.errors.append((storelocation,"Can't find coveragestore name in the coveragestore xml({})".format(storelocation)))
                continue

            store = m.group("name")

            m = workspaceid_re.search(data)
            if not m:
                self.errors.append(("{}({})".format(store,storelocation),"Can't find workspace id in the coveragestore({}({})) .".format(store,storelocation)))
                continue

            workspaceid = m.group("id")
            if workspaceid not in self.workspaceids:
                self.errors.append(("{}({})".format(store,storelocation),"The workspace id '{}' to which the coveragestore({}({})) belongs doesn't exist".format(workspaceid,store,storelocation)))
                continue

            workspace = self.workspaceids[workspaceid][0]

            if previous_workspace != workspace:
                if previous_workspace and count > 0:
                    print("Load {1} coveragestores in workspace '{0}'".format(previous_workspace,count))

                previous_workspace = workspace
                count = 0
            m = coveragestoreid_re.search(data)
            if not m:
                self.errors.append(("{}:{}".format(workspace,store),"Can't find coveragestore id in coveragestore({}:{}({}))".format(workspace,store,storelocation)))
                continue

            count += 1
            storeid = m.group("id")
            self.coveragestoreids[storeid] = (workspace,store,storelocation)

        if previous_workspace and count > 0:
            print("Load {1} coveragestores in workspace '{0}'".format(workspace,count))

        print("Load {} coveragestores".format(len(self.coveragestoreids)))


    def _featuretypes_xml(self):
        """
        A generator to return (featuretypelocation,xmldata)
        """
        workspacespath = os.path.join(self.geoserver_data_dir,"workspaces")
        if not os.path.exists(workspacespath):
            return

        for workspace in os.listdir(workspacespath):
            if workspace == "styles":
                continue
            elif not os.path.isdir(os.path.join(workspacespath,workspace)):
                #not a workspace
                continue
            else:
                workspacepath = os.path.join(workspacespath,workspace)

                for store in os.listdir(os.path.join(workspacepath)):
                    if not os.path.isdir(os.path.join(workspacepath,store)):
                        continue

                    storepath = os.path.join(workspacepath,store)
                    for featuretype in os.listdir(storepath):
                        if not os.path.isdir(os.path.join(storepath,featuretype)):
                            continue

                        featuretypefile = os.path.join(storepath,featuretype,"featuretype.xml")
                        if not os.path.exists(featuretypefile):
                            continue

                        with open(featuretypefile) as f:
                            yield (featuretypefile,f.read())

    def _load_featuretypes(self,geoserver):
        previous_workspace = None
        previous_store = None
        count_per_ws = 0
        total_count = 0
        count_per_store = 0
  
        for featuretypelocation,data in self._featuretypes_xml():
            m = name_re.search(data)
            if not m:
                self.errors.append((featuretypelocation,"Can't find featuretype name in the featuretype xml({})".format(featuretypelocation)))
                continue

            featuretype = m.group("name")

            m = namespaceid_re.search(data)
            if not m:
                self.errors.append(("{}({})".format(featuretype,featuretypelocation),"Can't find namespace id in the featuretype({}({})) .".format(featuretype,featuretypelocation)))
                continue

            namespaceid = m.group("id")
            if namespaceid not in self.namespaceids:
                self.errors.append(("{}({})".format(featuretype,featuretypelocation),"The namespace id '{}' to which the featuretype({}({})) belongs doesn't exist".format(namespaceid,featuretype,featuretypelocation)))
                continue

            namespace = self.namespaceids[namespaceid][0]
            workspace = namespace

            m = datastoreid_re.search(data)
            if not m:
                self.errors.append(("{}:{}".format(workspace,featuretype),"Can't find datastore id in featuretype({}:{}({}))".format(workspace,featuretype,featuretypelocation)))
                continue

            datastoreid = m.group("id")
            if datastoreid not in self.datastoreids:
                self.errors.append(("{}:{}".format(workspace,featuretype),"The datastore id({3}) to which the featuretype({0}:{1}({2})) belongs doesn't exist'".format(workspace,featuretype,featuretypelocation,datastoreid)))
                continue

            store = self.datastoreids[datastoreid][1]

            if previous_workspace != workspace:
                if previous_workspace and count_per_ws > 0:
                    print("Load {1} featuretypes in workspace '{0}'".format(previous_workspace,count_per_ws))
                if previous_store and count_per_store > 0:
                    print("Load {2} featuretypes in datastore '{0}:{1}'".format(previous_workspace,previous_store,count_per_store))

                previous_workspace = workspace
                count_per_ws = 0
                previous_store = store
                count_per_store = 0
            elif previous_store != store:
                if previous_store and count_per_store > 0:
                    print("Load {2} featuretype in datastore '{0}:{1}'".format(previous_workspace,previous_store,count_per_store))

                previous_store = store
                count_per_store = 0

            m = featuretypeid_re.search(data)
            if not m:
                self.errors.append(("{}:{}:{}".format(workspace,store,featuretype),"Can't find featuretype id in featuretype '{}:{}:{}({})'".format(workspace,store,featuretype,featuretypelocation)))
                continue

            count_per_ws += 1
            count_per_store += 1
            total_count += 1
            featuretypeid = m.group("id")
            self.featuretypeids[featuretypeid] = (workspace,store,featuretype,featuretypelocation)
            if workspace not in self.featuretypes:
                self.featuretypes[workspace] = {}
            if store not in self.featuretypes[workspace]:
                self.featuretypes[workspace][store] = {}

            self.featuretypes[workspace][store][featuretype] = (featuretypeid,featuretypelocation)

        if previous_workspace and count_per_ws > 0:
            print("Load {1} featuretypes in workspace '{0}'".format(previous_workspace,count_per_ws))
        if previous_store and count_per_store > 0:
            print("Load {2} featuretypes in datastore '{0}:{1}'".format(previous_workspace,previous_store,count_per_store))

        if total_count > 0:
            print("Load {} featuretypes".format(total_count))

    def _wmslayers_xml(self):
        """
        A generator to return (wmslayerlocation,xmldata)
        """
        workspacespath = os.path.join(self.geoserver_data_dir,"workspaces")
        if not os.path.exists(workspacespath):
            return
        for workspace in os.listdir(workspacespath):
            if workspace == "styles":
                continue
            elif not os.path.isdir(os.path.join(workspacespath,workspace)):
                #not a workspace
                continue
            else:
                workspacepath = os.path.join(workspacespath,workspace)
                for store in os.listdir(os.path.join(workspacepath)):
                    if not os.path.isdir(os.path.join(workspacepath,store)):
                        continue
                    storepath = os.path.join(workspacepath,store)
                    for wmslayer in os.listdir(storepath):
                        if not os.path.isdir(os.path.join(storepath,wmslayer)):
                            continue

                        wmslayerfile = os.path.join(storepath,wmslayer,"wmslayer.xml")
                        if not os.path.exists(wmslayerfile):
                            continue

                        with open(wmslayerfile) as f:
                            yield (wmslayerfile,f.read())

    def _load_wmslayers(self,geoserver):
        previous_workspace = None
        previous_store = None
        count_per_ws = 0
        total_count = 0
        count_per_store = 0
  
        for layerlocation,data in self._wmslayers_xml():
            m = name_re.search(data)
            if not m:
                self.errors.append((layerlocation,"Can't find wmslayer name in the wmslayer xml({})".format(layerlocation)))
                continue

            wmslayer = m.group("name")

            m = namespaceid_re.search(data)
            if not m:
                self.errors.append(("{}({})".format(wmslayer,layerlocation),"Can't find namespace id in the wmslayer({}({})) .".format(wmslayer,layerlocation)))
                continue

            namespaceid = m.group("id")
            if namespaceid not in self.namespaceids:
                self.errors.append(("{}({})".format(wmslayer,layerlocation),"The namespace id '{}' to which the wmslayer({}({})) belongs doesn't exist".format(namespaceid,wmslayer,layerlocation)))
                continue

            namespace = self.namespaceids[namespaceid][0]
            workspace = namespace

            m = wmsstoreid_re.search(data)
            if not m:
                self.errors.append(("{}:{}".format(workspace,wmslayer),"Can't find wmsstore id in wmslayer({}:{}({}))".format(workspace,wmslayer,layerlocation)))
                continue

            wmsstoreid = m.group("id")
            if wmsstoreid not in self.wmsstoreids:
                self.errors.append(("{}:{}".format(workspace,wmslayer),"The wmsstore id({3}) to which the wmslayer({0}:{1}({2})) belongs doesn't exist'".format(workspace,wmslayer,layerlocation,wmsstoreid)))
                continue

            store = self.wmsstoreids[wmsstoreid][1]

            if previous_workspace != workspace:
                if previous_workspace and count_per_ws > 0:
                    print("Load {1} wmslayers in workspace '{0}'".format(previous_workspace,count_per_ws))
                if previous_store and count_per_store > 0:
                    print("Load {2} wmslayers in datastore '{0}:{1}'".format(previous_workspace,previous_store,count_per_store))

                previous_workspace = workspace
                count_per_ws = 0
                previous_store = store
                count_per_store = 0
            elif previous_store != store:
                if previous_store and count_per_store > 0:
                    print("Load {2} wmslayers in datastore '{0}:{1}'".format(previous_workspace,previous_store,count_per_store))

                previous_store = store
                count_per_store = 0

            m = wmslayerid_re.search(data)
            if not m:
                self.errors.append(("{}:{}:{}".format(workspace,store,wmslayer),"Can't find wmslayer id in wmslayer({}:{}:{})".format(workspace,store,wmslayer)))
                continue

            count_per_ws += 1
            count_per_store += 1
            total_count += 1
            wmslayerid = m.group("id")
            self.wmslayerids[wmslayerid] = (workspace,store,wmslayer,layerlocation)
            if workspace not in self.wmslayers:
                self.wmslayers[workspace] = {}
            if store not in self.wmslayers[workspace]:
                self.wmslayers[workspace][store] = {}
            self.wmslayers[workspace][store][wmslayer] = (wmslayerid,layerlocation)

        if previous_workspace and count_per_ws > 0:
            print("Load {1} wmslayers in workspace '{0}'".format(previous_workspace,count_per_ws))
        if previous_store and count_per_store > 0:
            print("Load {2} wmslayers in datastore '{0}:{1}'".format(previous_workspace,previous_store,count_per_store))

        if total_count > 0:
            print("Load {} wmslayers".format(total_count))

    def _coveragelayers_xml(self):
        """
        A generator to return (coveragelayerlocation,xmldata)
        """
        workspacespath = os.path.join(self.geoserver_data_dir,"workspaces")
        if not os.path.exists(workspacespath):
            return
        for workspace in os.listdir(workspacespath):
            if workspace == "styles":
                continue
            elif not os.path.isdir(os.path.join(workspacespath,workspace)):
                #not a workspace
                continue
            else:
                workspacepath = os.path.join(workspacespath,workspace)
                for store in os.listdir(os.path.join(workspacepath)):
                    if not os.path.isdir(os.path.join(workspacepath,store)):
                        continue
                    storepath = os.path.join(workspacepath,store)
                    for coveragelayer in os.listdir(storepath):
                        if not os.path.isdir(os.path.join(storepath,coveragelayer)):
                            continue

                        coveragelayerfile = os.path.join(storepath,coveragelayer,"coverage.xml")
                        if not os.path.exists(coveragelayerfile):
                            continue

                        with open(coveragelayerfile) as f:
                            yield (coveragelayerfile, f.read())

    def _load_coveragelayers(self,geoserver):
        previous_workspace = None
        previous_store = None
        count_per_ws = 0
        total_count = 0
        count_per_store = 0
  
        for coveragelayerlocation,data in self._coveragelayers_xml():
            m = name_re.search(data)
            if not m:
                self.errors.append((coveragelayerlocation,"Can't find coveragelayer name in the coveragelayer xml({})".format(coveragelayerlocation)))
                continue

            coveragelayer = m.group("name")

            m = namespaceid_re.search(data)
            if not m:
                self.errors.append(("{}({})".format(coveragelayer,coveragelayerlocation),"Can't find namespace id in the coveragelayer({}({})) .".format(coveragelayer,coveragelayerlocation)))
                continue

            namespaceid = m.group("id")
            if namespaceid not in self.namespaceids:
                self.errors.append(("{}({})".format(coveragelayer,coveragelayerlocation),"The namespace id '{}' to which the coveragelayer({}({})) belongs doesn't exist".format(namespaceid,coveragelayer,coveragelayerlocation)))
                continue

            namespace = self.namespaceids[namespaceid][0]
            workspace = namespace

            m = coveragestoreid_re.search(data)
            if not m:
                self.errors.append(("{}:{}:{}".format(workspace,coveragelayer),"Can't find store id in converagelayer({}:{}({}))".format(workspace,coveragelayer,coveragelayerlocation)))
                continue

            coveragestoreid = m.group("id")
            if coveragestoreid not in self.coveragestoreids:
                self.errors.append(("{}:{}".format(workspace,coveragelayer),"The coveragestore id({3}) to which the coveragelayer({0}:{1}({2})) belongs doesn't exist'".format(workspace,coveragelayer,coveragelayerlocation,coveragestoreid)))
                continue

            store = self.coveragestoreids[coveragestoreid][1]

            if previous_workspace != workspace:
                if previous_workspace and count_per_ws > 0:
                    print("Load {1} coveragelayers in workspace '{0}'".format(previous_workspace,count_per_ws))
                if previous_store and count_per_store > 0:
                    print("Load {2} coveragelayers in datastore '{0}:{1}'".format(previous_workspace,previous_store,count_per_store))

                previous_workspace = workspace
                count_per_ws = 0
                previous_store = store
                count_per_store = 0
            elif previous_store != store:
                if previous_store and count_per_store > 0:
                    print("Load {2} coveragelayers in datastore '{0}:{1}'".format(previous_workspace,previous_store,count_per_store))

                previous_store = store
                count_per_store = 0

            m = coveragelayerid_re.search(data)
            if not m:
                self.errors.append(("{}:{}:{}".format(workspace,store,coveragelayer),"Can't find coveragelayer id in converagelayer({}:{}:{})".format(workspace,store,coveragelayer)))
                continue

            count_per_ws += 1
            count_per_store += 1
            total_count += 1
            coveragelayerid = m.group("id")
            self.coveragelayerids[coveragelayerid] = (workspace,store,coveragelayer,coveragelayerlocation)
            if workspace not in self.coveragelayers:
                self.coveragelayers[workspace] = {}
            if store not in self.coveragelayers[workspace]:
                self.coveragelayers[workspace][store] = {}
            self.coveragelayers[workspace][store][coveragelayer] = (coveragelayerid,coveragelayerlocation)


        if previous_workspace and count_per_ws > 0:
            print("Load {1} coveragelayers in workspace '{0}'".format(previous_workspace,count_per_ws))
        if previous_store and count_per_store > 0:
            print("Load {2} coveragelayers in datastore '{0}:{1}'".format(previous_workspace,previous_store,count_per_store))

        if total_count > 0:
            print("Load {} coveragelayers".format(total_count))

    def _layers_xml(self):
        """
        A generator to return (layerlocation,xmldata)
        """
        workspacespath = os.path.join(self.geoserver_data_dir,"workspaces")
        if not os.path.exists(workspacespath):
            return
        for workspace in os.listdir(workspacespath):
            if workspace == "styles":
                continue
            elif not os.path.isdir(os.path.join(workspacespath,workspace)):
                #not a workspace
                continue
            else:
                workspacepath = os.path.join(workspacespath,workspace)
                for store in os.listdir(os.path.join(workspacepath)):
                    if not os.path.isdir(os.path.join(workspacepath,store)):
                        continue
                    storepath = os.path.join(workspacepath,store)
                    for layer in os.listdir(storepath):
                        if not os.path.isdir(os.path.join(storepath,layer)):
                            continue

                        layerfile = os.path.join(storepath,layer,"layer.xml")
                        if not os.path.exists(layerfile):
                            continue

                        with open(layerfile) as f:
                            yield (layerfile, f.read())

    def _load_layers(self,geoserver):
        previous_workspace = None
        previous_store = None
        count_per_ws = 0
        total_count = 0
        count_per_store = 0
  
        for layerlocation,data in self._layers_xml():
            m = name_re.search(data)
            if not m:
                self.errors.append((layerlocation,"Can't find coveragelayer name in the coveragelayer xml({})".format(layerlocation)))
                continue

            layer = m.group("name")

            workspace = None
            store = None
            m = featuretypeid_re.search(data)
            if m:
                featuretypeid = m.group("id")
                if featuretypeid not in self.featuretypeids:
                    self.errors.append(("{}({})".format(layer,layerlocation),"The featuretype id({2}) associated with layer({0}({1})) doesn't exist'".format(layer,layerlocation,featuretypeid)))
                    continue
                workspace = self.featuretypeids[featuretypeid][0]
                store = self.featuretypeids[featuretypeid][0]
            else:
                m = wmslayerid_re.search(data)
                if m:
                    wmslayerid = m.group("id")
                    if wmslayerid not in self.wmslayerids:
                        self.errors.append(("{}({})".format(layer,layerlocation),"The wmslayer id({2}) associated with layer({0}({1})) doesn't exist'".format(layer,layerlocation,wmslayerid)))
                        continue
                    workspace = self.wmslayerids[wmslayerid][0]
                    store = self.wmslayerids[wmslayerid][0]

            if not workspace:
                m = coveragelayerid_re.search(data)
                if m:
                    coveragelayerid = m.group("id")
                    if coveragelayerid not in self.coveragelayerids:
                        self.errors.append(("{}({})".format(layer,layerlocation),"The coveragelayer id({2}) associated with layer({0}({1})) doesn't exist'".format(layer,layerlocation,coveragelayerid)))
                        continue
                    workspace = self.coveragelayerids[coveragelayerid][0]
                    store = self.coveragelayerids[coveragelayerid][0]

            if not workspace:
                self.errors.append(("{}({})".format(layer,layerlocation),"Can't find the associated layer id (featuretype,wmslayer or converagelayer) in  layer xml ({0}({1})) doesn't exist'".format(layer,layerlocation)))
                continue
                
            if previous_workspace != workspace:
                if previous_workspace and count_per_ws > 0:
                    print("Load {1} layers in workspace '{0}'".format(previous_workspace,count_per_ws))
                if previous_store and count_per_store > 0:
                    print("Load {2} layers in datastore '{0}:{1}'".format(previous_workspace,previous_store,count_per_store))

                previous_workspace = workspace
                count_per_ws = 0
                previous_store = store
                count_per_store = 0
            elif previous_store != store:
                if previous_store and count_per_store > 0:
                    print("Load {2} layers in datastore '{0}:{1}'".format(previous_workspace,previous_store,count_per_store))

                previous_store = store
                count_per_store = 0

            m = layerid_re.search(data)
            if not m:
                self.errors.append(("{}:{}:{}".format(workspace,store,layer),"Can't find the layer id in layer({}:{}:{}({}))".format(workspace,store,layer,layerlocation)))
                continue

            count_per_ws += 1
            count_per_store += 1
            total_count += 1
            layerid = m.group("id")
            self.layerids[layerid] = (workspace,store,layer,layerlocation)
            self.layers[(workspace,layer)] = (layerid,layerlocation)

        if previous_workspace and count_per_ws > 0:
            print("Load {1} layers in workspace '{0}'".format(previous_workspace,count_per_ws))
        if previous_store and count_per_store > 0:
            print("Load {2} layers in datastore '{0}:{1}'".format(previous_workspace,previous_store,count_per_store))

        if total_count > 0:
            print("Load {} layers".format(total_count))

    def _layergroups_xml(self):
        """
        A generator to return (layergrouplocation,xmldata)
        """
        workspacespath = os.path.join(self.geoserver_data_dir,"workspaces")
        if not os.path.exists(workspacespath):
            return
        for workspace in os.listdir(workspacespath):
            if workspace == "styles":
                continue
            elif not os.path.isdir(os.path.join(workspacespath,workspace)):
                #not a workspace
                continue
            else:
                workspacepath = os.path.join(workspacespath,workspace)
                layergroupspath = os.path.join(workspacepath,"layergroups")
                if not os.path.exists(layergroupspath) or not os.path.isdir(layergroupspath):
                    continue

                for layergroupfile in os.listdir(layergroupspath):
                    if not layergroupfile.endswith(".xml"):
                        #not a layergroup xml file
                        continue
                    layergroup = layergroupfile[:-4]
                    layergroupfile = os.path.join(layergroupspath,layergroupfile)

                    with open(layergroupfile) as f:
                        yield (layergroupfile,f.read())

    def _load_layergroups(self,geoserver):
        previous_workspace = None
        count = 0
        for layergrouplocation,data in self._layergroups_xml():
            m = name_re.search(data)
            if not m:
                self.errors.append((layergrouplocation,"Can't find layergroup name in the layergroup xml({})".format(layergrouplocation)))
                continue

            layergroup = m.group("name")

            m = workspaceid_re.search(data)
            if not m:
                self.errors.append(("{}({})".format(layergroup,layergrouplocation),"Can't find workspace id in layergoup({}({}))".format(layergroup,layergrouplocation)))
                continue

            workspaceid = m.group("id")
            if workspaceid not in self.workspaceids:
                self.errors.append(("{}({})".format(layergroup,layergrouplocation),"The namespace id({3}) to which the layergroup({0}:{1}({2})) belongs doesn't exist'".format(workspaceid,layergroup,layergrouplocation,workspaceid)))
                continue

            workspace = self.workspaceids[workspaceid][0]
 
            if previous_workspace != workspace:
                if previous_workspace and count > 0:
                    print("Load {1} coveragestores in workspace '{0}'".format(previous_workspace,count))

                previous_workspace = workspace
                count = 0

            m = layergroupid_re.search(data)
            if not m:
                self.errors.append(("{}:{}".format(workspace,wmslayer),"Can't find layergroup id in layergoup({}:{}({}))".format(workspace,layergroup,layergrouplocation)))
                continue

            count += 1
            layergroupid = m.group("id")
            self.layergroupids[layergroupid] = (workspace,layergroup,layergrouplocation)
            if workspace not in self.layergroups:
                self.layergroups[workspace] = {}
            self.layergroups[workspace][layergroup] = (layergroupid,layergrouplocation)


        for layergrouplocation,data in self._layergroups_xml():
            m = name_re.search(data)
            if not m:
                continue

            layergroup = m.group("name")

            m = workspaceid_re.search(data)
            if not m:
                continue

            workspaceid = m.group("id")
            if workspaceid not in self.workspaceids:
                continue

            workspace = self.workspaceids[workspaceid][0]
 
            #load all member layers
            start = 0
            while True:
                m = layerid_re.search(data,start)
                if not m:
                    break
                layerid = m.group("id")
                if layerid not in self.layerids:
                    self.errors.append(("{}:{}".format(workspace,layergroup),"The member layer id({2}) of the layergroup({0}:{1})  doesn't exist'".format(workspace,layergroup,workspaceid)))
                start = m.end()


            #Load all member layer groups
            start = data.find("<publishables>")
            if start < 0:
                self.errors.append(("{}:{}}".format(workspace,layergroup),"Can't find '<pubishables>' in layergroup({}:{})".format(workspace,layergroup)))
            else:
                while True:
                    m = layergroupid_re.search(data,start)
                    if not m:
                        break
                    layergroupid1 = m.group("id")
                    if layergroupid1 == layergroupid:
                        self.errors.append(("{}:{}".format(workspace,layergroup),"The layergroup can't add itself as a member layer in layergroup({}:{})".format(workspace,layergroup)))
                    elif layergroupid1 not in self.layergroupids:
                        self.errors.append(("{}:{}".format(workspace,layergroup),"The member layergroup id({2}) of the layergroup({0}:{1}) doesn't exist'".format(workspace,layergroup,layergroupid1)))
                    start = m.end()

            #Load styles used by member layer
            start = 0
            while True:
                m = styleid_re.search(data,start)
                if not m:
                    break
                styleid = m.group("id")
                if styleid not in self.styleids:
                    self.errors.append(("{}:{}".format(workspace,layergroup),"The style id({2}) used by layergroup({0}:{1}) doesn't exist'".format(workspace,layergroup,styleid)))
                start = m.end()

        if previous_workspace and count > 0:
            print("Load {1} layergroups in workspace '{0}'".format(workspace,count))

        print("Load {} layergroups".format(len(self.layergroupids)))
                        
    def _styles_xml(self):
        """
        A generator to return (stylelocation,xmldata)
        """
        if os.path.exists(os.path.join(self.geoserver_data_dir,"styles")):
            for style in os.listdir(os.path.join(self.geoserver_data_dir,"styles")):
                if not style.endswith(".xml"):
                    continue
                stylefile = os.path.join(self.geoserver_data_dir,"styles",style)
                with open(stylefile) as f:
                    yield (stylefile,f.read())

        workspacespath = os.path.join(self.geoserver_data_dir,"workspaces")
        if not os.path.exists(workspacespath):
            return
        #load styles in workspace
        for workspace in os.listdir(workspacespath):
            if workspace == "styles":
                #default styles
                for style in os.listdir(os.path.join(workspacespath,"styles")):
                    if not style.endswith(".xml"):
                        continue
                    stylefile = os.path.join(workspacespath,"styles",style)
                    with open(stylefile) as f:
                        yield (stylefile,f.read())

            elif not os.path.isdir(os.path.join(self.geoserver_data_dir,"workspaces",workspace)):
                #not a workspace
                continue
            else:
                workspacepath = os.path.join(self.geoserver_data_dir,"workspaces",workspace)

                if os.path.exists(os.path.join(workspacepath,"styles")) and os.path.isdir(os.path.join(workspacepath,"styles")):
                    for style in os.listdir(os.path.join(workspacepath,"styles")):
                        if not style.endswith(".xml"):
                            continue
                        stylefile = os.path.join(workspacepath,"styles",style)
                        with open(stylefile) as f:
                            yield (stylefile,f.read())

    def _load_styles(self,geoserver):
        previous_workspace = None
        count = 0
        workspace = None
        for stylelocation,data in self._styles_xml():
            m = name_re.search(data)
            if not m:
                self.errors.append((stylelocation,"Can't find style name in the style xml({})".format(stylelocation)))
                continue
            style = m.group("name")

            m = workspaceid_re.search(data)
            if  m:
                workspaceid = m.group("id")
                if workspaceid not in self.workspaceids:
                    self.errors.append(("{}({})".format(style,stylelocation),"The workspace id '{}' to which the style({}({})) belongs doesn't exist".format(style,stylelocation)))
                    workspace = workspaceid
                else:
                    workspace = self.workspaceids[workspaceid][0]

            else:
                workspace = ""

            m = styleid_re.search(data)
            if not m:
                self.errors.append(("{}({})".format(style,stylelocation),"Can't find style id in style xml file({}({}))".format(style,stylelocation)))
                continue

            if previous_workspace != workspace:
                if previous_workspace is not None and count > 0:
                    print("Load {1} styles in workspace '{0}'".format(previous_workspace,count))

                previous_workspace = workspace
                count = 0

            count += 1
            styleid = m.group("id")
            self.styleids[styleid] = (workspace,style,stylelocation)

        if previous_workspace and count > 0:
            print("Load {1} layergroups in workspace '{0}'".format(workspace,count))

        print("Load {} layergroups".format(len(self.styleids)))
                        

    def _gwclayers_xml(self):
        """
        A generator to return (gwclayerlocation,xmldata)
        """
        gwclayersdir = os.path.join(self.geoserver_data_dir,"gwc-layers")
        if not os.path.exists(gwclayersdir):
            return

        for gwclayerfile in os.listdir(gwclayersdir):
            if not gwclayerfile.endswith(".xml"):
                continue
            gwclayerfile = os.path.join(gwclayersdir,gwclayerfile)
            with open(gwclayerfile) as f:
                yield (gwclayerfile, f.read())


    def _del_orphan_gwclayer(self,geoserver,gwclayername,gwclayerlocation,gwclayerid,workspace,layername):
        if geoserver.has_gwclayer(workspace,layername):
            #gwc layer exists
            geoserver.delete_gwclayer(workspace,layername)
            if geoserver.has_gwclayer(workspace,layername):
                self.errors.append((gwclayername,"The layergroup({0}) associated with the gwclayer({0}({1}))  doesn't exist".format(gwclayername,gwclayerlocation)))
                self.errors.append("GWC Layer({}:{}) : Failed to clean the orphan gwc layer.".format(workspace,layername))
                if gwclayerid not in self.layergroupids:
                    self.errors.append((gwclayername,"The layergroupid({2}) associated with the gwclayer({0}({1})) doesn't exist".format(gwclayername,gwclayerlocation,gwclayerid)))
            else:
                self.cleaned_datas.append("GWC Layer({}:{}) : Succeed to clean the orphan gwc layer.".format(workspace,layername))
        else:
            #gwc layer doesn't exists
            utils.remove_file(gwclayerlocation)
            self.cleaned_datas.append("GWC Layer({}:{}) : Succeed to delete the orphan gwc layer file({}).".format(workspace,layername,gwclayerlocation))

    def _load_gwclayers(self,geoserver):
        for gwclayerlocation,data in self._gwclayers_xml():
            m = gwclayername_re.search(data)
            if not m:
                self.errors.append((gwclayerlocation,"Can't find gwclayer name in gwclayer xml file location({})".format(gwclayerlocation)))
                continue
            gwclayername = m.group("name")
            if ":" in  gwclayername:
                workspace,layername = gwclayername.split(":",1)
            else:
                workspace  = ""
                layername = gwclayername

            if workspace not in self.gwclayers:
                self.gwclayers[workspace] = {}
            self.gwclayers[workspace][layername] = gwclayerlocation
            
            m = gwclayerid_re.search(data)
            if not m:
                self.errors.append(("{}({})".format(gwclayername,gwclayerlocation),"Can't find gwclayer id in gwclayer xml file location({}({}))".format(gwclayername,gwclayerlocation)))
                continue

            gwclayerid = m.group("id")
            if gwclayerid.lower().startswith("layergroup"):
                layergroup = self.layergroups.get(workspace,{}).get(layername)
                if not layergroup:
                    self._del_orphan_gwclayer(geoserver,gwclayername,gwclayerlocation,gwclayerid,workspace,layername)
                else:
                    if gwclayerid not in self.layergroupids:
                        self.errors.append((gwclayername,"The layergroupid({2}) associated with the gwclayer({0}({1})) doesn't exist".format(gwclayername,gwclayerlocation,gwclayerid)))
                    if layergroup[0] != gwclayerid:
                        self.errors.append((gwclayername,"The layergorupid({3}) of the layergroup({1})  doesn't match the layergroupid({2}) associated with gwclayer({0}({1})) ".format(gwclayername,gwclayerlocation,gwclayerid,layergroup[0])))
            else:
                layer = self.layers.get((workspace,layername))
                if not layer:
                    self._del_orphan_gwclayer(geoserver,gwclayername,gwclayerlocation,gwclayerid,workspace,layername)
                else:
                    if gwclayerid not in self.layerids:
                        self.errors.append((gwclayername,"The layerid({2}) associated with the gwclayer({0}({1})) doesn't exist".format(gwclayername,gwclayerlocation,gwclayerid)))
                    if layer[0] != gwclayerid:
                        self.errors.append((gwclayername,"The layerid({3}) of the layer({0})  doesn't match the layerid({2}) associated with gwclayer({0}({1})) ".format(gwclayername,gwclayerlocation,gwclayerid,layer[0])))

        count = 0
        for workspace,workspacedata in self.gwclayers.items():
            print("Load {1} gwc layers in workspace({0})".format(workspace,len(workspacedata)))
            count += len(workspacedata)
        print("Load {} gwc layers".format(count))


    def check(self,geoserver,print_result=True):
        if not self.enabled:
            print("Data Consistency Check is not enabled")
            return
        self._load_workspaces(geoserver)
        self._load_namespaces(geoserver)
        self._load_styles(geoserver)
        self._load_datastores(geoserver)
        self._load_wmsstores(geoserver)
        self._load_coveragestores(geoserver)
        self._load_featuretypes(geoserver)
        self._load_wmslayers(geoserver)
        self._load_coveragelayers(geoserver)
        self._load_layers(geoserver)
        self._load_layergroups(geoserver)
        self._load_gwclayers(geoserver)

        if not print_result:
            return

        if self.cleaned_datas:
            print("""Deleted {} orphan resources from geoserver
    {}""".format(
        len(self.cleaned_datas),
        "\n    ".join( "{} : {}".format(*msg) for msg in self.cleaned_datas)
        ))

        if self.errors:
            print("""Found {} errorss
    {}""".format(
        len(self.errors),
        "\n    ".join( "{} : {}".format(*msg) for msg in self.errors)
        ))
        else:
            print("No errors found")

        if self.errors:
            print("""Found {} errorss
    {}""".format(
        len(self.errors),
        "\n    ".join( "{} : {}".format(*msg) for msg in self.errors)
        ))
        else:
            print("No errors found")
class GeoserverDataConsistencyCheck4JdbcConfig(GeoserverDataConsistencyCheck):

    def __init__(self,data_dir=None,host=None,port=None,dbname=None,user=None,passwd=None,sslmode=None):
        super().__init__(data_dir=data_dir)
        self.host = host or os.environ.get("GEOSERVER_CATALOG_HOST") or "localhost"
        self.port = port or int(os.environ.get("GEOSERVER_CATALOG_PORT",5432))
        self.dbname = dbname or os.environ.get("GEOSERVER_CATALOG_DB")
        self.user = user or os.environ.get("GEOSERVER_CATALOG_USER") or ""
        self.passwd = passwd or os.environ.get("GEOSERVER_CATALOG_PASSMODE") or ""
        self.sslmode = sslmode or os.environ.get("GEOSERVER_CATALOG_SSLMODE") or "prefer"

    @property
    def enabled(self):
        return super().enabled and self.host and self.port and self.dbname

    def _retrieve_xmldata(self,typename):
        with psycopg.connect("dbname='{2}' user='{3}' password='{4}' host='{0}' port={1}".format(self.host,self.port,self.dbname,self.user,self.passwd or "",self.sslmode)) as conn:
            with conn.cursor() as cur:
                cur.execute("select a.oid,a.id,a.blob from object a join type b on a.type_id = b.oid where b.typename='{}'".format(typename))
                for row in cur.fetchall():
                    yield (row[1],row[2])
            
    def _workspaces_xml(self):
        return self._retrieve_xmldata("org.geoserver.catalog.WorkspaceInfo")

    def _namespaces_xml(self):
        return self._retrieve_xmldata("org.geoserver.catalog.NamespaceInfo")

    def _datastores_xml(self):
        return self._retrieve_xmldata("org.geoserver.catalog.DataStoreInfo")

    def _wmsstores_xml(self):
        return self._retrieve_xmldata("org.geoserver.catalog.WMSStoreInfo")

    def _coveragestores_xml(self):
        return self._retrieve_xmldata("org.geoserver.catalog.CoverageStoreInfo")
        
    def _featuretypes_xml(self):
        return self._retrieve_xmldata("org.geoserver.catalog.FeatureTypeInfo")

    def _wmslayers_xml(self):
        return self._retrieve_xmldata("org.geoserver.catalog.WMSLayerInfo")

    def _coveragelayers_xml(self):
        return self._retrieve_xmldata("org.geoserver.catalog.CoverageInfo")

    def _layers_xml(self):
        return self._retrieve_xmldata("org.geoserver.catalog.LayerInfo")

    def _layergroups_xml(self):
        return self._retrieve_xmldata("org.geoserver.catalog.LayerGroupInfo")

    def _styles_xml(self):
        return self._retrieve_xmldata("org.geoserver.catalog.StyleInfo")

def check(print_result=True):
    geoserver_name = os.environ["GEOSERVER_NAME"]
    geoserver_url = os.environ["GEOSERVER_URL"]
    geoserver_user = os.environ["GEOSERVER_USER"]
    geoserver_password = os.environ["GEOSERVER_PASSWORD"]
    geoserver_ssl_verify = os.environ.get("GEOSERVER_SSL_VERIFY","true").lower() == "true"
    geoserver = Geoserver(geoserver_url,geoserver_user,geoserver_password,headers=settings.GET_REQUEST_HEADERS("GEOSERVER_REQUEST_HEADERS"),ssl_verify=geoserver_ssl_verify)

    if os.environ.get("GEOSERVER_CATALOG_DB"):
        consistencycheck = GeoserverDataConsistencyCheck4JdbcConfig()
    else:
        consistencycheck = GeoserverDataConsistencyCheck()
    consistencycheck.check(geoserver,print_result=print_result)

class GeoserverDataConsistencyCheckTask(Task):
    category = "Geoserver Data Consistency Check"
    consistencycheck = None

    def __init__(self,data_dir=None,host=None,port=None,dbname=None,user=None,passwd=None,sslmode=None,post_actions_factory = None):
        super().__init__(post_actions_factory=post_actions_factory)
        self.data_dir = data_dir or os.environ.get("GEOSERVER_DATA_DIR")
        self.host = host or os.environ.get("GEOSERVER_CATALOG_HOST") or "localhost"
        self.port = port or int(os.environ.get("GEOSERVER_CATALOG_PORT",5432))
        self.dbname = dbname or os.environ.get("GEOSERVER_CATALOG_DB")
        self.user = user or os.environ.get("GEOSERVER_CATALOG_USER") or ""
        self.passwd = passwd or os.environ.get("GEOSERVER_CATALOG_PASSWORD") or ""
        if sslmode is None:
            self.sslmode = os.environ.get("GEOSERVER_CATALOG_SSLMODE") or "prefer"
        else:
            self.sslmode = sslmode

        if self.dbname:
            self.consistencycheck = GeoserverDataConsistencyCheck4JdbcConfig(data_dir=self.data_dir,host=self.host,port=self.port,dbname=self.dbname,user=self.user,passwd=self.passwd,sslmode=self.sslmode)
        else:
            self.consistencycheck = GeoserverDataConsistencyCheck(data_dir=self.data_dir)
        self.attempts = 1

    def _format_result(self):
        if not self.consistencycheck.enabled:
            return "Data Consistency Check Not Enabled."
        gwclayers = 0
        for workspacedata in self.consistencycheck.gwclayers.values():
            gwclayers += len(workspacedata)

        return """Load {} workspaces
Load {} namespaces
Load {} datastores
Load {} featuretypes
Load {} wmsstores
Load {} wmslayers
Load {} coveragestores
Load {} coveragelayers
Load {} layergroups
Load {} styles
Load {} gwclayers
Cleaned {} orphan resources
    {}
""".format(
    len(self.consistencycheck.workspaceids),
    len(self.consistencycheck.namespaceids),
    len(self.consistencycheck.datastoreids),
    len(self.consistencycheck.featuretypeids),
    len(self.consistencycheck.wmsstoreids),
    len(self.consistencycheck.wmslayerids),
    len(self.consistencycheck.coveragestoreids),
    len(self.consistencycheck.coveragelayerids),
    len(self.consistencycheck.layergroupids),
    len(self.consistencycheck.styleids),
    gwclayers,
    len(self.consistencycheck.cleaned_datas),
    "\n    ".join(self.consistencycheck.cleaned_datas)
)

    def _warnings(self):
        if not self.consistencycheck.warnings and not self.consistencycheck.errors:
            return
        
        level = self.ERROR if self.consistencycheck.errors else self.WARNING
        msg = ""
        if self.consistencycheck.warnings:
            msg = """Found {} warnings:
    {}
""" .format(len(self.consistencycheck.warnings),"\n    ".join("{}: {}".format(*d) for d in self.consistencycheck.warnings))
        if self.consistencycheck.errors:
            msg = """{}Found {} errors:
    {}
""" .format(msg,len(self.consistencycheck.errors),"\n    ".join("{}: {}".format(*d) for d in self.consistencycheck.errors))
        yield (level,msg) 

    def _exec(self,geoserver):
        self.consistencycheck.check(geoserver,False)
        return self.consistencycheck

if __name__ == '__main__':
    check()

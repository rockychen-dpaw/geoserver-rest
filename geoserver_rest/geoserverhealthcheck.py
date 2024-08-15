import os
import threading
import logging

from .taskrunner import TaskRunner
from .geoserver import Geoserver
from .tasks import *
from .csv import CSVWriter
from . import settings
from . import timezone
from . import loggingconfig

logger = logging.getLogger("geoserver_rest.geoserverhealthcheck")

actions_map = {

}

class GeoserverHealthCheck(object):
    
    def __init__(self,geoserver_name,geoserver_url,geoserver_user,geoserver_password,reportfile,warningfile,headers=None,dop=1):
        self.geoserver = Geoserver(geoserver_url,geoserver_user,geoserver_password,headers=headers)
        self.taskrunner = TaskRunner(geoserver_name,self.geoserver,dop=dop)
        self.reportfile = reportfile
        self.warningfile = warningfile
        self._reportwriteaction = None
        self._warningwriteaction = None
        self.warnings = 0
        self.errors = 0
        self.dop = dop
        self.starttime = None
        self.endtime = None

    @property
    def tasks(self):
        return self.taskrunner.total_tasks

    def synced_taskaction(self,taskaction):
        lock = threading.Lock() if self.dop > 1 else None
        def _func(task):
            try:
                lock.acquire(blocking=True, timeout=-1)
                taskaction(task)
            finally:
                lock.release()

        return _func if self.dop > 1 else taskaction

    def write_report_action_factory(self,basetask):
        reportwriter = CSVWriter(self.reportfile,header=basetask.reportheader)
        def _func(task):
            reportwriter.writerows(task.reportrows())

        return self.synced_taskaction(_func)
        
    def write_warning_action_factory(self,basetask):
        warningwriter = CSVWriter(self.warningfile,header=basetask.warningheader)
        def _func(task):
            for warning in task.warnings():
                if warning[2] == "Warning":
                    self.warnings += 1
                else:
                    self.errors += 1
                warningwriter.writerow(warning)
        
        return self.synced_taskaction(_func)

    def create_tasks_from_previoustask_factory(self,f_createtasks,limit=None):
        if limit is None:
            #limit = 2 if settings.DEBUG else 0
            limit = 0
        def _func(previoustask):
            for t in f_createtasks(previoustask,limit = limit):
               self.taskrunner.add_task(t)

        return _func

    def post_actions_factory(self,taskcls):
        if taskcls == ListWorkspaces:
            return [
                self.create_tasks_from_previoustask_factory(createtasks_ListResourcesInWorkspace,0),
                self._reportwriteaction,
                self._warningwriteaction
            ]
        elif taskcls == ListResourcesInWorkspace:
            return [
                self.create_tasks_from_previoustask_factory(createtasks_ListFeatureTypes),
                self.create_tasks_from_previoustask_factory(createtasks_ListWMSLayers),
                self.create_tasks_from_previoustask_factory(createtasks_GetLayergroupDetail),
                self._reportwriteaction,
                self._warningwriteaction
            ]
        elif taskcls == ListDatastores:
            return [
                self.create_tasks_from_previoustask_factory(createtasks_ListFeatureTypes),
                self._reportwriteaction,
                self._warningwriteaction
            ]
        elif taskcls == ListFeatureTypes:
            return [
                #self.create_tasks_from_previoustask_factory(createtasks_GetFeatureCount),
                self.create_tasks_from_previoustask_factory(createtasks_GetFeatureTypeDetail),
                self._reportwriteaction,
                self._warningwriteaction
            ]
        elif taskcls == ListWMSStores:
            return [
                self.create_tasks_from_previoustask_factory(createtasks_ListWMSLayers),
                self._reportwriteaction,
                self._warningwriteaction,
            ]
        elif taskcls == ListWMSLayers:
            return [
                self.create_tasks_from_previoustask_factory(createtasks_GetWMSLayerDetail),
                self._reportwriteaction,
                self._warningwriteaction,
            ]
        elif taskcls == ListLayergroups:
            return [
                self.create_tasks_from_previoustask_factory(createtasks_GetLayergroupDetail),
                self._reportwriteaction,
                self._warningwriteaction,
            ]
        elif taskcls == GetFeatureTypeDetail:
            return [
                self.create_tasks_from_previoustask_factory(createtasks_GetFeatures),
                #self.create_tasks_from_previoustask_factory(createtasks_TestWMSService4FeatureType),
                #self.create_tasks_from_previoustask_factory(createtasks_TestWMTSService4FeatureType),
                self._reportwriteaction,
                self._warningwriteaction,
            ]
        elif taskcls == GetWMSLayerDetail:
            return [
                self.create_tasks_from_previoustask_factory(createtasks_TestWMSService4WMSLayer),
                self.create_tasks_from_previoustask_factory(createtasks_TestWMTSService4WMSLayer),
                self._reportwriteaction,
                self._warningwriteaction,
            ]
        elif taskcls == GetLayergroupDetail:
            return [
                self.create_tasks_from_previoustask_factory(createtasks_TestWMSService4Layergroup),
                self.create_tasks_from_previoustask_factory(createtasks_TestWMTSService4Layergroup),
                self._reportwriteaction,
                self._warningwriteaction,
            ]
        elif taskcls == GetFeatureCount:
            return [
                self._reportwriteaction,
                self._warningwriteaction,
            ]
        elif taskcls == GetFeatures:
            return [
                self.create_tasks_from_previoustask_factory(createtasks_TestWMSService4Feature),
                self.create_tasks_from_previoustask_factory(createtasks_TestWMTSService4Feature),
                self._reportwriteaction,
                self._warningwriteaction,
            ]
        elif taskcls == TestWMSService4FeatureType:
            return [
                self._reportwriteaction,
                self._warningwriteaction,
            ]
        elif taskcls == TestWMTSService4FeatureType:
            return [
                self._reportwriteaction,
                self._warningwriteaction,
            ]
        elif taskcls == TestWMSService4WMSLayer:
            return [
                self._reportwriteaction,
                self._warningwriteaction,
            ]
        elif taskcls == TestWMTSService4WMSLayer:
            return [
                self._reportwriteaction,
                self._warningwriteaction,
            ]
        elif taskcls == TestWMSService4Layergroup:
            return [
                self._reportwriteaction,
                self._warningwriteaction,
            ]
        elif taskcls == TestWMTSService4Layergroup:
            return [
                self._reportwriteaction,
                self._warningwriteaction,
            ]
        elif taskcls == WFSGetCapabilitiesTask:
            return [
                self._reportwriteaction,
                self._warningwriteaction,
            ]
        elif taskcls == WMSGetCapabilitiesTask:
            return [
                self._reportwriteaction,
                self._warningwriteaction,
            ]
        elif taskcls == WMTSGetCapabilitiesTask:
            return [
                self._reportwriteaction,
                self._warningwriteaction,
            ]


        return None
        
        

    def start(self):
        self.taskrunner.start()
        task = ListWorkspaces(post_actions_factory = self.post_actions_factory)
        self._reportwriteaction = self.write_report_action_factory(task)
        self._warningwriteaction = self.write_warning_action_factory(task)
        self.starttime = timezone.localtime()
        self.taskrunner.add_task(WFSGetCapabilitiesTask(post_actions_factory = self.post_actions_factory))
        self.taskrunner.add_task(WMSGetCapabilitiesTask(post_actions_factory = self.post_actions_factory))
        self.taskrunner.add_task(WMTSGetCapabilitiesTask(post_actions_factory = self.post_actions_factory))
        self.taskrunner.add_task(task)


    def wait_to_finish(self):
        self.taskrunner.wait_to_shutdown()
        self.endtime = timezone.localtime()
        logger.warning("Start Time : {}, End Time : {}, Spent : {} , Tasks : {} ,  Warnings : {} , Errors : {}".format(
            timezone.format(self.starttime,pattern="%Y-%m-%d %H:%M:%S.%f"),
            timezone.format(self.endtime,pattern="%Y-%m-%d %H:%M:%S.%f"),
            timezone.format_timedelta(self.endtime - self.starttime),
            self.tasks,
            self.warnings,
            self.errors
        ))



if __name__ == '__main__':
    geoserver_name = os.environ["GEOSERVER_NAME"]
    geoserver_url = os.environ["GEOSERVER_URL"]
    geoserver_user = os.environ["GEOSERVER_USER"]
    geoserver_password = os.environ["GEOSERVER_PASSWORD"]
    requestheaders = os.environ.get("GEOSERVER_REQUESTHEADERS")
    reportfile = os.environ.get("GEOSERVER_REPORTFILE") or "geoserver_report.csv"
    warningfile = os.environ.get("GEOSERVER_WARNINGFILE") or "geoserver_warning.csv"
    healthcheck_dop = int(os.environ.get("HEALTHCHECK_DOP",2))
    if requestheaders:
        requestheaders = dict([(header.strip().split("=",1) if "=" in header else [header,""]) for header in requestheaders.split(",") if header.strip()])
    healthcheck = GeoserverHealthCheck(geoserver_name,geoserver_url,geoserver_user,geoserver_password,reportfile,warningfile,requestheaders,healthcheck_dop)
    healthcheck.start()
    healthcheck.wait_to_finish()



import os
import threading
import logging

from .taskrunner import TaskRunner
from .geoserver import Geoserver
from .tasks import *
from .csv import CSVWriter
from . import settings

logger = logging.getLogger(__name__)

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
        self.dop = dop

    def synced_taskaction(self,taskaction):
        lock = threading.Lock() if self.dop > 1 else None
        def _func(task):
            try:
                lock.acquire(blocking=True, timeout=-1)
                taskaction(task)
            finally:
                lock.release()

        return _func if self.dop > 1 else taskaction

    def create_tasks_from_previoustask_factory(self,f_createtasks):
        def _func(previoustask):
            for t in f_createtasks(previoustask):
               self.taskrunner.add_task(t)

        return _func

    def post_actions_factory(self,task_category):
        if task_category == ListWorkspaces.category:
            return [
                self.create_tasks_from_previoustask_factory(createtasks_ListDatastores),
                self.create_tasks_from_previoustask_factory(createtasks_ListWMSstores),
                self._reportwriteaction,
                self._warningwriteaction
            ]
        elif task_category == ListDatastores.category:
            return [
                self.create_tasks_from_previoustask_factory(createtasks_ListFeatureTypes),
                self._reportwriteaction,
                self._warningwriteaction
            ]
        elif task_category == ListFeatureTypes.category:
            return [
                self.create_tasks_from_previoustask_factory(createtasks_GetFeatureCount),
                self.create_tasks_from_previoustask_factory(createtasks_GetFeatureTypeDetail),
                self._reportwriteaction,
                self._warningwriteaction
            ]
        elif task_category == ListWMSstores.category:
            return [
                self.create_tasks_from_previoustask_factory(createtasks_ListWMSLayers),
                self._reportwriteaction,
                self._warningwriteaction,
            ]
        elif task_category == ListWMSLayers.category:
            return [
                self.create_tasks_from_previoustask_factory(createtasks_GetWMSLayerDetail),
                self._reportwriteaction,
                self._warningwriteaction,
            ]
        elif task_category == GetFeatureTypeDetail.category:
            return [
                self._reportwriteaction,
                self._warningwriteaction,
            ]
        elif task_category == GetWMSLayerDetail.category:
            return [
                self._reportwriteaction,
                self._warningwriteaction,
            ]

        return None
        
        

    def start(self):
        self.taskrunner.start()
        task = ListWorkspaces(post_actions_factory = self.post_actions_factory)
        reportwriter = CSVWriter(self.reportfile,header=task.reportheader)
        warningwriter = CSVWriter(self.warningfile,header=task.warningheader)
        self._reportwriteaction =  self.synced_taskaction(reportwriter.write_taskreport)
        self._warningwriteaction = self.synced_taskaction(warningwriter.write_taskwarnings)

        self.taskrunner.add_task(task)


    def wait_to_finish(self):
        self.taskrunner.wait_to_shutdown()



if __name__ == '__main__':
    geoserver_name = os.environ["GEOSERVER_NAME"]
    geoserver_url = os.environ["GEOSERVER_URL"]
    geoserver_user = os.environ["GEOSERVER_USER"]
    geoserver_password = os.environ["GEOSERVER_PASSWORD"]
    requestheaders = os.environ.get("GEOSERVER_REQUESTHEADERS")
    reportfile = os.environ.get("GEOSERVER_REPORTFILE") or "geoserver_report.csv"
    warningfile = os.environ.get("GEOSERVER_WARNINGFILE") or "geoserver_warning.csv"
    geoserver_healthcheck_dop = int(os.environ.get("GEOSERVER_HEALTHCHECK_DOP","1"))
    if requestheaders:
        requestheaders = dict([(header.strip().split("=",1) if "=" in header else [header,""]) for header in requestheaders.split(",") if header.strip()])
    healthcheck = GeoserverHealthCheck(geoserver_name,geoserver_url,geoserver_user,geoserver_password,reportfile,warningfile,requestheaders,geoserver_healthcheck_dop)
    healthcheck.start()
    healthcheck.wait_to_finish()



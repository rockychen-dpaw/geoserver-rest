import os

from .taskrunner import TaskRunner
from .geoserver import Geoserver
from .tasks import *

actions_map = {

}

class GeoserverHealthCheck(object):
    def __init__(self,geoserver_name,geoserver_url,geoserver_user,geoserver_password,headers=None,dop=1):
        self.geoserver = Geoserver(geoserver_url,geoserver_user,geoserver_password,headers=headers)
        self.taskrunner = TaskRunner(geoserver_name,self.geoserver,dop=dop)

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
                self.create_tasks_from_previoustask_factory(createtasks_ListWMTSLayers)
            ]
        elif task_category == ListDatastores.category:
            return [
                self.create_tasks_from_previoustask_factory(createtasks_ListFeatureTypes)
            ]
        elif task_category == ListFeatureTypes.category:
            return [
                self.create_tasks_from_previoustask_factory(createtasks_GetFeatureTypeStyles)
            ]
        elif task_category == ListWMSstores.category:
            return [
                self.create_tasks_from_previoustask_factory(createtasks_ListWMSLayers)
            ]


        return None
        
        

    def start(self):
        self.taskrunner.start()
        self.taskrunner.add_task(ListWorkspaces(post_actions_factory = self.post_actions_factory))


    def wait_to_finish(self):
        self.taskrunner.wait_to_shutdown()



if __name__ == '__main__':
    geoserver_name = os.environ["GEOSERVER_NAME"]
    geoserver_url = os.environ["GEOSERVER_URL"]
    geoserver_user = os.environ["GEOSERVER_USER"]
    geoserver_password = os.environ["GEOSERVER_PASSWORD"]
    headers = os.environ.get("GEOSERVER_HEADERS")
    geoserver_healthcheck_dop = int(os.environ.get("GEOSERVER_HEALTHCHECK_DOP","1"))
    if headers:
        headers = dict([(header.strip().split("=",1) if "=" in header else [header,""]) for header in headers.split(",") if header.strip()])
    healthcheck = GeoserverHealthCheck(geoserver_name,geoserver_url,geoserver_user,geoserver_password,headers,geoserver_healthcheck_dop)
    healthcheck.start()
    healthcheck.wait_to_finish()



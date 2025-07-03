import os
import queue
import threading
import logging
import shutil
import jinja2
import traceback

from .taskrunner import TaskRunner
from .geoserver import Geoserver
from .tasks import *
from .csv import CSVWriter
from . import settings
from . import timezone
from . import loggingconfig
from . import utils
from .mail import EmailMessage

logger = logging.getLogger("geoserver_rest.geoserverhealthcheck")

jinja_env = jinja2.Environment(
    loader=jinja2.FileSystemLoader([settings.BASE_DIR]),
    autoescape=jinja2.select_autoescape()
)

class GeoserverHealthCheck(object):
    report_file = None
    warnings_file = None
    reportwriter = None
    warningwriter = None
    _finished_tasks = None
    metadata = None
    
    def __init__(self,geoserver_name,geoserver_url,geoserver_user,geoserver_password,requestheaders=None,dop=1,keep_tasks=False):
        self.keep_tasks = keep_tasks
        self.geoserver_name = geoserver_name
        self.geoserver = Geoserver(geoserver_url,geoserver_user,geoserver_password,headers=requestheaders)
        self.taskrunner = TaskRunner(geoserver_name,self.geoserver,dop=dop,keep_tasks=keep_tasks)
        self._reportwriteaction = None
        self._warningwriteaction = None
        self.warnings = 0
        self.errors = 0
        self.dop = dop
        self.starttime = None
        self.endtime = None
        self._report_dir = None
        self._reports_home = None

    @property
    def finished_tasks(self):
        return self._finished_tasks

    @property
    def reports_home(self):
        if not self._reports_home:
            self._reports_home = os.path.join(settings.REPORT_HOME,self.geoserver_name)
            if os.path.exists(self._reports_home):
                if not os.path.isdir(self._reports_home):
                    os.remove(self._reports_home)

            if not os.path.exists(self._reports_home):
                os.makedirs(self._reports_home,mode=0o755)

        return self._reports_home
    @property
    def report_dir(self):
        if not self._report_dir:
            self._report_dir = os.path.join(self.reports_home,timezone.format(self.starttime,pattern="%Y-%m-%dT%H-%M-%S"))
            if os.path.exists(self._report_dir):
                if os.path.isdir(self._report_dir):
                    os.rmdir(self._report_dir)
                else:
                    os.remove(self._report_dir)

            os.makedirs(self._report_dir,mode=0o755)

        return self._report_dir

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
        self.report_file = "{}_{}_report.csv".format(self.geoserver_name,timezone.format(self.starttime,pattern="%Y-%m-%dT%H-%M-%S"))
        self.reportwriter = CSVWriter(os.path.join(self.report_dir,self.report_file),header=basetask.reportheader)
        def _func(task):
            self.reportwriter.writerows(task.reportrows())

        return self.synced_taskaction(_func)
        
    def write_warning_action_factory(self,basetask):
        self.warnings_file = "{}_{}_warnings.csv".format(self.geoserver_name,timezone.format(self.starttime,pattern="%Y-%m-%dT%H-%M-%S"))
        self.warningwriter = CSVWriter(os.path.join(self.report_dir,self.warnings_file),header=basetask.warningheader)
        def _func(task):
            for warning in task.warnings():
                if warning[2] == "Warning":
                    self.warnings += 1
                else:
                    self.errors += 1
                self.warningwriter.writerow(warning)
        
        return self.synced_taskaction(_func)

    def create_tasks_from_previoustask_factory(self,f_createtasks,limit=None):
        if limit is None:
            limit = 0
        def _func(previoustask):
            for t in f_createtasks(previoustask,limit = limit):
               self.taskrunner.add_task(t)

        return _func

    def post_actions_factory(self,taskcls):
        if taskcls == CheckGeoserverAlive:
            return [
                self.create_tasks_from_previoustask_factory(createtasks_WFSGetCapabilities,0),
                self.create_tasks_from_previoustask_factory(createtasks_WMSGetCapabilities,0),
                self.create_tasks_from_previoustask_factory(createtasks_WMTSGetCapabilities,0),
                self.create_tasks_from_previoustask_factory(createtasks_ListWorkspaces,0),
                self._reportwriteaction,
                self._warningwriteaction
            ]
        elif taskcls == ListWorkspaces:
            return [
                self.create_tasks_from_previoustask_factory(createtasks_ListDatastores,0),
                self.create_tasks_from_previoustask_factory(createtasks_ListWMSstores,0),
                self.create_tasks_from_previoustask_factory(createtasks_ListLayergroups,0),
                self._reportwriteaction,
                self._warningwriteaction
            ]
        elif taskcls == ListDatastores:
            return [
                self.create_tasks_from_previoustask_factory(createtasks_GetDatastore),
                self._reportwriteaction,
                self._warningwriteaction
            ]
        elif taskcls == GetDatastore:
            return [
                self.create_tasks_from_previoustask_factory(createtasks_ListFeatureTypes),
                self._reportwriteaction,
                self._warningwriteaction
            ]
        elif taskcls == ListFeatureTypes:
            return [
                self.create_tasks_from_previoustask_factory(createtasks_GetFeatureTypeDetail),
                self._reportwriteaction,
                self._warningwriteaction
            ]
        elif taskcls == ListWMSstores:
            return [
                self.create_tasks_from_previoustask_factory(createtasks_GetWMSstore),
                self._reportwriteaction,
                self._warningwriteaction,
            ]
        elif taskcls == GetWMSstore:
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
                #self.create_tasks_from_previoustask_factory(createtasks_GetFeatureCount),
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
        self.starttime = timezone.localtime()
        #basetask = CheckGeoserverAlive()
        basetask = GetWMSLayerDetail("kaartdijin-boodja-private","WA_firescan","linescanner_SWIR_3-6_2",self.geoserver.get_wmsstore("kaartdijin-boodja-private","WA_firescan"))
        self._reportwriteaction = self.write_report_action_factory(basetask)
        self._warningwriteaction = self.write_warning_action_factory(basetask)

        #task = CheckGeoserverAlive(post_actions_factory = self.post_actions_factory)
        task = GetFeatureTypeDetail("kaartdijin-boodja-private","CPT_DFES_BUSHFIRE_PRONE_AREAS","CPT_DFES_BUSHFIRE_PRONE_AREAS",self.geoserver.get_datastore("kaartdijin-boodja-private","CPT_DFES_BUSHFIRE_PRONE_AREAS"),post_actions_factory = self.post_actions_factory)
        self.taskrunner.add_task(task)


    def close_report_writers(self):
        try:
            self.reportwriter.close()
            self.reportwriter = None
            self.warningwriter.close()
            self.warningwriter = None
        except Exception as ex:
            exceptions.append(ex)

    def wait_to_finish(self,close_report_writer=True):
        """
        return the processing metadata
        """
        exceptions = []
        try:
            self.taskrunner.wait_to_shutdown()
            if close_report_writer:
                self.close_report_writers()
        except Exception as ex:
            exceptions.append(ex)

        if self.taskrunner.finished_tasks:
            logger.debug("Begin to populate the finished task map.size={}".format(self.taskrunner.finished_tasks.qsize()))
            self._finished_tasks = {}
            index = 0
            while True:
                try:
                    task = self.taskrunner.finished_tasks.get(block=False)
                except queue.Empty as ex:
                    task = None
                index += 1
                if not task:
                    break
                key = (task.category,utils.toMapKey(task.keyparameters))
                self._finished_tasks[key] = task
            logger.debug("End to populate the finished task map.size={}".format(len(self._finished_tasks)))

        self.endtime = timezone.localtime()
        self.metadata = {
            "starttime": timezone.format(self.starttime,pattern="%Y-%m-%d %H:%M:%S.%f"),
            "endtime": timezone.format(self.endtime,pattern="%Y-%m-%d %H:%M:%S.%f"),
            "exectime": timezone.format_timedelta(self.endtime - self.starttime),
            "total_tasks": self.tasks if self.tasks is not None else "-",
            "warnings": self.warnings if self.warnings is not None else "-",
            "errors": self.errors if self.errors is not None else "-",
            "report_dir": os.path.basename(self.report_dir) if self.report_dir else "-",
            "report_file":self.report_file if self.report_file else "-",
            "warnings_file":self.warnings_file if self.warnings_file else "-",
        }
        if exceptions:
            self.metadata["exceptions"] = "\r\n----------------------------------------------------\r\n".join(
                "\r\n".join(traceback.format_exception(type(ex),ex,ex.__traceback__)) for ex in exceptions
            )


    def write_report(self):
        if not self.metadata:
            raise Exception("Please call this method after healcheck is finished.")
        try:
            with open(os.path.join(self.report_dir,"reportmeta.json"),'w') as f:
                f.write(json.dumps(self.metadata,indent=4))
    
            if os.path.exists(os.path.join(settings.BASE_DIR,"reports.html")):
                #write the reports.html
                reports = []
                for f in os.listdir(self.reports_home):
                    if not os.path.isdir(os.path.join(self.reports_home,f)):
                        #not a report folder
                        continue
    
                    if not os.path.exists(os.path.join(self.reports_home,f,"reportmeta.json")):
                        #meta file doesn't exist
                        continue
                    with open(os.path.join(self.reports_home,f,"reportmeta.json"),'rt') as f:
                        try:
                            reports.append(json.loads(f.read()))
                        except :
                            logger.error("Failed to load report meta data.{}".format(traceback.format_exc()))
    
                #sort the reports
                reports.sort(key=lambda d:d["starttime"],reverse=True)
                #remove the expired reports
                if len(reports) > settings.MAX_REPORTS:
                    for i in range(len(reports) - settings.MAX_REPORTS):
                        try:
                            shutil.rmtree(os.path.join(self.reports_home,reports[-1]["report_dir"]))
                        except:
                            logger.error("Failed to delete expired report folder({}).{}".format(reports[-1]["report_dir"],traceback.format_exc()))
                        del reports[-1]
                #generate the reports.html
                reports_template = jinja_env.get_template("reports.html")
                with open(os.path.join(self.reports_home,"reports.html"),"w") as f:
                    f.write(reports_template.render({
                        "geoserver_name" : self.geoserver_name,
                        "geoserver_url" : self.geoserver.geoserver_url,
                        "reports":reports
                    }))
    
            logger.warning("Geoserver : {}, Start Time : {}, End Time : {}, Spent : {} , Tasks : {} ,  Warnings : {} , Errors : {}".format(
                self.geoserver_name,
                timezone.format(self.starttime,pattern="%Y-%m-%d %H:%M:%S.%f"),
                timezone.format(self.endtime,pattern="%Y-%m-%d %H:%M:%S.%f"),
                timezone.format_timedelta(self.endtime - self.starttime),
                self.tasks,
                self.warnings,
                self.errors
            ))

        except Exception as ex:
            logger.error("Failed to write the report.{}".format(traceback.format_exc()))
            
if __name__ == '__main__':
    geoserver_name = os.environ["GEOSERVER_NAME"]
    geoserver_url = os.environ["GEOSERVER_URL"]
    geoserver_user = os.environ["GEOSERVER_USER"]
    geoserver_password = os.environ["GEOSERVER_PASSWORD"]
    if not geoserver_name:
        geoserver_name = utils.get_domain(geoserver_url)

    healthcheck = GeoserverHealthCheck(geoserver_name,geoserver_url,geoserver_user,geoserver_password,settings.REQUEST_HEADERS,settings.HEALTHCHECK_DOP)
    healthcheck.start()
    healthcheck.wait_to_finish()
    healthcheck.write_report()

    if settings.EMAIL_ENABLED and (healthcheck.metadata.get("exceptions") or healthcheck.errors):
        #send email
        subject = "Some errors found on geoserver({})".format(geoserver_name)
        context = {"healthchecks":[{"healthcheck":healthcheck,"processing_metadata":healthcheck.metadata}]}
        #generate the email body
        body_template = jinja_env.get_template("notify_email.html")
        body = body_template.render(context)
        email = EmailMessage(subject=subject,body=body,from_email=settings.EMAIL_FROM,to=settings.EMAIL_TO,cc=settings.EMAIL_CC,bcc=settings.EMAIL_BCC)
        email.content_subtype = 'html'
        email.attach_file(os.path.join(healthcheck.report_dir, healthcheck.warnings_file))
        email.send()



from datetime import datetime
import traceback
import requests
import json
import time
import logging

from .. import timezone
from .. import settings

logger = logging.getLogger(__name__)

class Task(object):
    WARNING = "Warning"
    ERROR = "Error"
    starttime = None
    endtime = None
    exceptions = None
    category = None

    result = None

    arguments = None
    keyarguments = None
    post_actions_factory = None

    trieddata = None
    taskrunner = None

    _messages = None
    def __init__(self,post_actions_factory = None):
        self.attempts = settings.TASK_ATTEMPTS.get(self.__class__.__name__,settings.TASK_ATTEMPTS.get("__DEFAULT__",1))
        if self.attempts < 1:
            self.attempts = 1
        if post_actions_factory:
            self.post_actions_factory = post_actions_factory

    @property
    def parameters(self):
        """
        A generator to return the parameter tuple (name,value)
        """
        if self.arguments:
            for arg in self.arguments :
                v = getattr(self,arg)
                yield (arg,"" if v is None else v)

    @property
    def keyparameters(self):
        """
        A generator to return the parameter tuple (name,value)
        """
        if self.keyarguments:
            for arg in self.keyarguments :
                v = getattr(self,arg)
                yield (arg,"" if v is None else v)

    def format_parameters(self,separator=","):
        return separator.join("{}={}".format(k,json.dumps(v)) for k,v in self.parameters)

    @property
    def triedmsg(self):
        if self.trieddata:
            return """=======================================
{} more attempts:
    {}
""".format(len(self.trieddata),"\n    ---------------------\n    ".join( "Start Time : {} , End Time : {} , Exec time : {} seconds , Exception : {}".format(timezone.format(starttime,"%Y-%m-%d %H:%M:%S.%f"),timezone.format(endtime,"%Y-%m-%d %H:%M:%S.%f"),(endtime - starttime).total_seconds() if starttime and endtime else "",str(ex)) for starttime,endtime,ex in self.trieddata))
        else:
            return None
            

    @property
    def exec_result(self):
        msg = None
        try:
            msg = self._format_result()
        except:
            pass

        if msg:
            if self.exceptions:
               msg = "{}\r\n{}".format(msg,"\r\n".join("{}({})".format(ex.__class__.__name__,str(ex)) for ex in self.exceptions))
        elif self.exceptions:
            msg = "\r\n".join("{}({})".format(ex.__class__.__name__,str(ex)) for ex in self.exceptions)

        triedmsg = self.triedmsg
        if triedmsg:
            if msg:
                return "{}\n{}".format(msg,triedmsg)
            else:
                return triedmsg
        else:
            return msg

    def _format_result(self):
        return json.dumps(self.result) if self.result else ""

    @property
    def reportheader(self):
        return ["Task Category","Task Parameters","Status","Execute Starttime","Execute Endtime","Process Time","Execute Result"]

    def reportrows(self):
        yield (self.category,
            self.format_parameters("\r\n"),
            self.status,
            timezone.format(self.starttime,"%Y-%m-%d %H:%M:%S.%f") if self.starttime else "",
            timezone.format(self.endtime,"%Y-%m-%d %H:%M:%S.%f") if self.endtime else "",
            " {}".format(timezone.format_timedelta((self.endtime - self.starttime) if self.starttime and self.endtime else None,ignore_milliseconds=False)),
            self.exec_result
        )

    @property
    def warningheader(self):
        return ["Task Category","Task Parameters","Level","Execute Starttime","Execute Endtime","Process Time","Message"]

    def warnings(self):
        """
        Report the execute exception and the warnings and errors from task result
        """
        triedmsg = self.triedmsg

        if self.exceptions:
            msg = "\r\n".join( "{}({})".format(ex.__class__.__name__,str(ex)) if isinstance(ex,requests.RequestException) else "\r\n".join(traceback.format_exception(type(ex),ex    ,ex.__traceback__)) for ex in self.exceptions)
            try:
                url = self.url
            except:
                url = None
            if url and url not in msg:
                msg = "URL: {}\r\n{}".format(url,msg)

            if triedmsg:
                msg = "{}\n{}".format(msg,triedmsg)

            yield (self.category,
                self.format_parameters("\r\n"),
                self.ERROR,
                timezone.format(self.starttime,"%Y-%m-%d %H:%M:%S.%f") if self.starttime else "",
                timezone.format(self.endtime,"%Y-%m-%d %H:%M:%S.%f") if self.endtime else "",
                " {}".format(timezone.format_timedelta((self.endtime - self.starttime) if self.starttime and self.endtime else None,ignore_milliseconds=False)),
                msg
            )

        add_triedmsg = True if triedmsg else False
        if self.is_succeed:
            for level,msg in (self._messages or []):
                if add_triedmsg:
                    add_triedmsg = False
                    msg = "{}\n{}".format(msg,triedmsg)

                yield (self.category,
                    self.format_parameters("\r\n"),
                    level,
                    timezone.format(self.starttime,"%Y-%m-%d %H:%M:%S.%f") if self.starttime else "",
                    timezone.format(self.endtime,"%Y-%m-%d %H:%M:%S.%f") if self.endtime else "",
                    " {}".format(timezone.format_timedelta((self.endtime - self.starttime) if self.starttime and self.endtime else None,ignore_milliseconds=False)),
                    msg
                )

            for level,msg in (self._warnings() or []):
                if add_triedmsg:
                    add_triedmsg = False
                    msg = "{}\n{}".format(msg,triedmsg)

                yield (self.category,
                    self.format_parameters("\r\n"),
                    level,
                    timezone.format(self.starttime,"%Y-%m-%d %H:%M:%S.%f") if self.starttime else "",
                    timezone.format(self.endtime,"%Y-%m-%d %H:%M:%S.%f") if self.endtime else "",
                    " {}".format(timezone.format_timedelta((self.endtime - self.starttime) if self.starttime and self.endtime else None,ignore_milliseconds=False)),
                    msg
                )

    def _warnings(self):
        """
        Report the warnings and errors from the task result
        return a generator to return (level,msg)
        """
        return None

    @property
    def status(self):
        if self.is_waiting:
            return "Waiting"
        elif self.is_running:
            return "Running"
        elif self.is_succeed:
            return "Succeed"
        else:
            return "Failed"

    @property
    def is_running(self):
        return True if self.starttime is not None and self.endtime is None else False

    @property
    def is_waiting(self):
        return True if self.starttime is None else False

    @property
    def is_finished(self):
        return True if (self.starttime is not None and self.endtime is not None) else False

    @property
    def is_succeed(self):
        return True if (self.starttime is not None and self.endtime is not None and  self.result is not None) else False

    @property
    def is_failed(self):
        return True if (self.starttime is not None and self.endtime is not None and  self.result is None) else False

    def run(self,geoserver):
        self.starttime = timezone.localtime()
        try:
            self.result = self._exec(geoserver)
            self.attempts -= 1
        except Exception as ex:
            if not isinstance(ex,requests.ConnectionError):
                #failed not because  geoserver is not ready, reduce the attempts by 1
                exmsg = str(ex).lower()
                if any(msg in exmsg for msg in [
                        "the requested style can not be used with this layer",
                        "unknown wkb type",
                        "errors while inspecting the location of an external graphic",
                        "unknown font",
                        "exceptioncode=\"tileoutofrange\"",
                        "http error code 401",
                        "could not find layer",
                        "could not locate a layer",
                        "rendering process failed"
                ]):
                    #this issues can't be fixed by retry
                    self.attempts = 0
                else:
                    self.attempts -= 1

            logger.error("{0}: Failed to process the task {1}.\n{2}".format(geoserver,self,traceback.format_exc()))
            if self.attempts < 1:
                #no more try
                self.exceptions = [ex]
                self.result = None
                logger.info("Failed to process task({1}({0}))\n{2}".format(geoserver,self,self.triedmsg))
            else:
                if not self.trieddata:
                    self.trieddata = [(self.starttime,timezone.localtime(),ex)]
                else:
                    self.trieddata.append((self.starttime,timezone.localtime(),ex))
                #readd this task as retry tasks
                logger.info("Task({1}({0})) is scheduled to retry".format(geoserver,self))
                self.taskrunner.add_retrytask(self)

                return
        finally:
            self.endtime = timezone.localtime()

        if self.post_actions_factory:
            post_actions = self.post_actions_factory(self.__class__)
            if post_actions:
                for action in post_actions:
                    try:
                       action(self)
                    except Exception as ex:
                        logger.error("{0} : Failed to execute the post action({1}). {2}".format(self.__class__.__name__,action.__class__.__name__,traceback.format_exc()))
                        if self.exceptions:
                            self.exceptions.append(ex)
                        else:
                            self.exceptions = [ex]
                        
    def _exec(self,geoserver):
        raise Exception("Not implemented")

    def __str__(self):
        return "{}({})".format(self.category,",".join("{}={}".format(k,json.dumps(v)) for k,v in self.keyparameters))


class OutOfSyncTask(Task):
    def __init__(self,task,missing=True):
        super().__init__()
        self.task = task
        self.missing = missing

    def _exec(self,geoserver):
        raise Exception("Not Supported")

    def reportrows(self):
        return

    def warnings(self):
        if self.missing:
            yield (self.task.category,
                self.task.format_parameters("\r\n"),
                self.ERROR,
                "",
                "",
                "",
                "Not exist in slave geoserver"
            )
        else:
            yield (self.task.category,
                self.task.format_parameters("\r\n"),
                self.WARNING,
                "",
                "",
                "",
                "Not exist in admin geoserver"
            )


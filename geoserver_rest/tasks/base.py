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
    queuetime = None
    starttime = None
    endtime = None
    exceptions = None
    category = None

    result = None

    arguments = None
    post_actions_factory = None

    def __init__(self,post_actions_factory = None):
        self.queuetime = timezone.localtime()
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

    def format_parameters(self,separator=","):
        return separator.join("{}={}".format(k,json.dumps(v)) for k,v in self.parameters)

    @property
    def exec_result(self):
        msg = ""
        if self.is_succeed:
            if self.exceptions:
               return "{}\r\n{}".format(self._format_result(),"\r\n".join("{}({})".format(ex.__class__.__name__,str(ex)) for ex in self.exceptions))
            else:
               return self._format_result()
        elif self.exceptions:
            return "\r\n".join("{}({})".format(ex.__class__.__name__,str(ex)) for ex in self.exceptions)
        else:
            return ""

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
            timezone.format_timedelta(self.endtime - self.starttime,ignore_milliseconds=False) if self.starttime and self.endtime else "",
            self.exec_result
        )

    @property
    def warningheader(self):
        return ["Task Category","Task Parameters","Level","Execute Starttime","Execute Endtime","Process Time","Message"]

    def warnings(self):
        """
        Report the execute exception and the warnings and errors from task result
        """
        if self.exceptions:
            msg = "\r\n".join( "{}({})".format(ex.__class__.__name__,str(ex)) if isinstance(ex,requests.RequestException) else "\r\n".join(traceback.format_exception(type(ex),ex    ,ex.__traceback__)) for ex in self.exceptions)
            try:
                url = self.url
            except:
                url = None
            if url and url not in msg:
                msg = "URL: {}\r\n{}".format(url,msg)

            yield (self.category,
                self.format_parameters("\r\n"),
                self.ERROR,
                timezone.format(self.starttime,"%Y-%m-%d %H:%M:%S.%f") if self.starttime else "",
                timezone.format(self.endtime,"%Y-%m-%d %H:%M:%S.%f") if self.endtime else "",
                (self.endtime - self.starttime).total_seconds() if self.starttime and self.endtime else "",
                msg
            )

        if self.is_succeed:
            for level,msg in (self._warnings() or []):
                yield (self.category,
                    self.format_parameters("\r\n"),
                    level,
                    timezone.format(self.starttime,"%Y-%m-%d %H:%M:%S.%f") if self.starttime else "",
                    timezone.format(self.endtime,"%Y-%m-%d %H:%M:%S.%f") if self.endtime else "",
                    (self.endtime - self.starttime).total_seconds() if self.starttime and self.endtime else "",
                    msg
            )

    def _warnings(self):
        """
        Report the warnings and errors from the task result
        return a generator to return (level,msg)
        """
        pass

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
            while True:
                try:
                    self.result = self._exec(geoserver)
                    break
                except requests.ConnectionError as ex:
                    if (timezone.localtime() - self.starttime).total_seconds() < settings.GEOSERVER_RESTART_TIMEOUT:
                        logger.error("The geoserver() is not available.".format(geoserver.geoserver_url))
                        time.sleep(10)
                    else:
                        raise
        except Exception as ex:
            logger.error(traceback.format_exc())
            self.exceptions = [ex]
            self.result = None
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



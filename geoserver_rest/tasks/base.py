from datetime import datetime
import traceback
import json

from .. import timezone

class Task(object):
    queuetime = None
    starttime = None
    endtime = None
    exception = None
    category = None

    result = None
    post_actions = None

    arguments = None
    post_actions_factory = None

    def __init__(self,post_actions_factory = None):
        self.queuetime = timezone.localtime()
        if post_actions_factory:
            self.post_actions_factory = post_actions_factory
            post_actions = self.post_actions_factory(self.category)
            if post_actions:
                self.post_actions = post_actions

    def add_postaction(self,post_action):
        """
        Add a post action or a list of post actions
        """
        if not post_action:
            return
        if isinstance(post_action,(list,tuple)):
            if not self.post_actions:
                self.post_actions = post_action
            else:
                if isinstance(self.post_actions,tuple):
                    self.post_actions = list(self.post_actions)
                for a in post_action:
                    self.post_actions.append(a)
        else:
            if not self.post_actions:
                self.post_actions = [post_action]
            else:
                if isinstance(self.post_actions,tuple):
                    self.post_actions = list(self.post_actions)
                self.post_actions.append(post_action)

    @property
    def parameters(self):
        """
        A generator to return the parameter tuple (name,value)
        """
        if self.arguments:
            for arg in self.arguments :
                if getattr(self,arg):
                    yield (arg,getattr(self,arg))

    def format_parameters(self,separator=","):
        return separator.join("{}={}".format(k,json.dumps(v)) for k,v in self.parameters)

    @property
    def exec_result(self):
        if self.is_failed:
            return str(self.exception)
        elif self.is_succeed:
            return self._format_result()
        else:
            return ""

    def _format_result(self):
        return json.dumps(self.result) if self.result else ""

    def report_rows(self):
        yield (self.category,
            self.format_parameters("\r\n"),
            self.status,
            timezone.format(self.starttime,"%Y-%m-%d %H:%M:%S.%f") if self.starttime else "",
            timezone.format(self.endtime,"%Y-%m-%d %H:%M:%S.%f") if self.endtime else "",
            self.exec_result
        )
    def warnings(self):
        """
        Report the execute exception and the warnings and errors from task result
        """
        if self.is_failed:
            yield (self.category,
                self.format_parameters("\r\n"),
                "Error",
                timezone.format(self.starttime,"%Y-%m-%d %H:%M:%S.%f") if self.starttime else "",
                timezone.format(self.endtime,"%Y-%m-%d %H:%M:%S.%f") if self.endtime else "",
                str(self.exception)
            )
        elif self.is_succeed:
            for r in self._warnings():
                yield r

    def _warnings(self):
        """
        Report the warnings and errors from the task result
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
        return True if (self.starttime is not None and self.endtime is not None and  self.exception is None) else False

    @property
    def is_failed(self):
        return True if (self.starttime is not None and self.endtime is not None and  self.exception is not None) else False

    def run(self,geoserver):
        self.starttime = timezone.localtime()
        try:
            self.result = self._exec(geoserver)
            if self.post_actions:
                for action in self.post_actions:
                    action(self)
        except Exception as ex:
            traceback.print_exc()
            self.exception = ex
        finally:
            self.endtime = timezone.localtime()

    def _exec(self,geoserver):
        raise Exception("Not implemented")



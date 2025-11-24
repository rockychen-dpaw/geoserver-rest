import threading
import queue
import collections
import logging
import time
from datetime import timedelta

from . import settings
from . import timezone

logger = logging.getLogger(__name__)

class TaskRunner(object):
    endtask = object()
    finished_tasks = None

    def __init__(self,name,dop=1,keep_tasks=False):
        """
        timeout: the timeout for getting task from queue
        dop: degree of parallelism
        """
        self.name = name
        self.dop = dop
        self.tasks = queue.Queue()
        self.retrytasks = queue.Queue()
        if keep_tasks:
            self.finished_tasks = queue.Queue()
        self.workers = None
        self.total_tasks = 0

    def add_task(self,task):
        """
        Should call this method in sync mode(in the same thread)
        """
        self.tasks.put(task)
        task.taskrunner = self
        self.total_tasks += 1
    
    def add_retrytask(self,task):
        """
        Should call this method in sync mode(in the same thread)
        """
        self.retrytasks.put(task)

    def readd_retrytasks(self):
        """
        move the retry tasks from queue 'retrytasks' to queue 'tasks'
        """
        if self.retrytasks.empty():
            return 0

        count = 0
        while True:
            try:
                self.tasks.put(self.retrytasks.get(block=False,timeout=None))
                count += 1
            except queue.Empty as ex:
                break
            except Exception as ex:
                logger.error("{} : Failed to move task from queue 'retrytasks' to queue 'tasks'.{}".format(self.name,ex))
                continue

        logger.info("{} : Move {} tasks from queue 'retrytasks' to queue 'tasks'".format(self.name,count))

        return count
    
    def wait_to_shutdown(self):
        """
        Wait until all tasks are finished and worker threads are terminated.
        """
        logger.info("{} : Wait all tasks to finish.".format(self.name))
        retry = 0
        while True:
            self.tasks.join()
            if self.retrytasks.empty():
                break
            else:
                retry += 1
                logger.info("{} : All tasks in the queue 'tasks' have been finished, wait {} seconds before the {}th time to retry the failed tasks".format(self.name,settings.TASK_RETRY_INTERVAL,retry))
                time.sleep(settings.TASK_RETRY_INTERVAL)
                count = self.readd_retrytasks()
                logger.info("{} : The {}th time to retry the failed {} tasks now".format(self.name,retry,count))
        logger.info("{} : All tasks({}) are finished.".format(self.name,self.total_tasks))

        for worker in self.workers:
            self.tasks.put(self.endtask)
        logger.info("{} : All tasks have been finish, wait runner workers to end".format(self.name))

        for worker in self.workers:
            worker.join()

        logger.info("{} : All runner workers have been end".format(self.name))

    def start(self):
        self.workers = []
        if self.dop == 1:
            self.workers.append(TaskWorker("TaskWorker-{}".format(self.name),self))
        else:
            for i in range(self.dop):
                self.workers.append(TaskWorker("TaskWorker-{}-{}".format(self.name,i + 1),self))

    def run_task(self,task):
        try:
            task.run()
        except Exception as ex:
            logger.error("Failed to run task {}.{}".format(task,str(ex)))


class TaskWorker(threading.Thread):
    shutdown = False
    def __init__(self,name,runner):
        super().__init__(name=name,daemon=False)
        self.runner = runner
        self.start()

    def run(self):
        logger.info("The task worker({}) is running.".format(self.name))
        while True:
            try:
                task = self.runner.tasks.get(block=True,timeout=None)
            except Exception as ex:
                logger.warning("The task worker({}) was interrupted.{}".format(self.name,str(ex)))
                break
            if task == self.runner.endtask:
                break
            else:
                logger.debug("{0} : Begin to run task {1})".format(self.name,task))
                try:
                    self.runner.run_task(task)
                    if self.runner.finished_tasks:
                        self.runner.finished_tasks.put(task)
                finally:
                    self.runner.tasks.task_done()
                logger.debug("{0} : End to run task {1}".format(self.name,task))

        logger.info("{} : The task worker is terminated.".format(self.name))
            

class GeoserverTaskRunner(TaskRunner):
    endtask = object()
    finished_tasks = None

    def __init__(self,name,geoserver,dop=1,keep_tasks=False):
        """
        timeout: the timeout for getting task from queue
        dop: degree of parallelism
        """
        super().__init__(name,dop=dop,keep_tasks=keep_tasks)
        self.geoserver = geoserver

    def run_task(self,task):
        try:
            task.run(self.geoserver)
        except Exception as ex:
            logger.error("Failed to run task {}.{}".format(task,str(ex)))


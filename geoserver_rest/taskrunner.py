import threading
import queue
import collections
import logging

logger = logging.getLogger(__name__)

class TaskRunner(object):
    endtask = object()
    finished_tasks = None

    def __init__(self,name,geoserver,dop=1,keep_tasks=False):
        """
        timeout: the timeout for getting task from queue
        dop: degree of parallelism
        """
        self.name = name
        self.dop = dop
        self.geoserver = geoserver
        self.tasks = queue.Queue()
        if keep_tasks:
            self.finished_tasks = queue.Queue()
        self.workers = None
        self.total_tasks = 0

    def add_task(self,task):
        """
        Should call this method in sync mode(in the same thread)
        """
        self.tasks.put(task)
        self.total_tasks += 1
    
    def wait_to_finish(self):
        """
        Wait until all tasks are finished
        """
        self.tasks.join()
        logger.info("All tasks({}) are finished.".format(self.total_tasks))

    def wait_to_shutdown(self):
        """
        Wait until all tasks are finished and worker threads are terminated.
        """
        self.wait_to_finish()
        for worker in self.workers:
            self.tasks.put(self.endtask)

        for worker in self.workers:
            worker.join()

    def start(self):
        self.workers = []
        if self.dop == 1:
            self.workers.append(TaskWorker("TaskWorker-{}".format(self.name),self))
        else:
            for i in range(self.dop):
                self.workers.append(TaskWorker("TaskWorker-{}-{}".format(self.name,i + 1),self))

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
            if not task:
                continue
            if task == self.runner.endtask:
                #end
                self.runner.tasks.task_done()
                break
            logger.debug("{0} : Begin to run task {1}({2})".format(self.name,task.category,task.format_parameters(", ")))
            task.run(self.runner.geoserver)
            if self.runner.finished_tasks:
                self.runner.finished_tasks.put(task)
            self.runner.tasks.task_done()
            logger.debug("{0} : {3} to run task {1}({2})".format(self.name,task.category,task.format_parameters(", "),"Succeed" if task.is_succeed else "Failed"))

        logger.info("The task worker({}) is terminated.".format(self.name))
            









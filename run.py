import re, os, time
import hashlib
from BackgroundTask.handler import get_task_class_list, get_task_class_instance
from BackgroundTask.interface import BackgroundTaskInterface
import logging

""" 
This class manages running tasks, keeps track that everything 
when it was called
from the cron script every minute
creates and manages lock file, etc
basic idea that task queues run all the time, 
every minute from cron we're just checking
if they're running, and if not starting them 
"""

class BackgroundTaskRunner:

    # static
    lock_files_path = os.path.dirname(os.path.abspath(__file__)) + '/lockfiles/'

    # vars
    task_class_string = None
    worker_number = 0
    worker_id = None
    worker_lock_file = None
    task_count = 0

    def __init__(self, task_class_string, worker_number):

        # checking if lock files folder exists
        if not os.path.exists(self.lock_files_path):
            raise PermissionError("Directory for lock files is not found")

        self.task_class_string = task_class_string
        self.worker_number = worker_number
        self.worker_id = hashlib.md5((self.task_class_string + str(self.worker_number)).encode()).hexdigest()
        self.worker_lock_file = self.lock_files_path + self.worker_id + '.lock'

    # --------------------------------------------------
    # MAIN
    # --------------------------------------------------

    def run(self):

        if self._is_worker_running():
            self._log("The process is already running. All well.", 'debug')
            return

        # delay is required so all worker wouldn't try to access the queue at once
        self._log("Starting.. Starting delay for worker [%d] is %d seconds.." % (self.worker_number, self.worker_number))
        time.sleep(self.worker_number)

        # creating lock file
        self._start_worker()

        task_class = get_task_class_instance(self.task_class_string)
        assert isinstance(task_class, BackgroundTaskInterface)

        while True:

            # get new task
            task_list = task_class.get_new_task_list(self.worker_number)

            # empty sleep
            if len(task_list) < 1:
                self._log("Sleep empty interval.. %d sec.." % task_class.empty_interval, 'debug')
                time.sleep(task_class.empty_interval)
                continue

            # do the tasks
            for task in task_list:
                task_class.do_work(task)
                self.task_count = self.task_count + 1

            # busy sleep
            self._log("Going for a busy interval, tasks done in total: %d" % self.task_count, 'debug')
            time.sleep(task_class.busy_interval)

    def stop(self):

        self._log("The worker was killed from manage.py", 'info')

        # no lock file - nothing to kill
        if not os.path.exists(self.worker_lock_file):
            return

        # opening the lock file and to check if the pid is active
        lock_file = open(self.worker_lock_file, 'r')
        pid = BackgroundTaskRunner._format_int(lock_file.read())

        try:
            os.kill(pid, 9)
        except OSError:
            pass

    # --------------------------------------------------
    # MAIN PROTECTED
    # --------------------------------------------------

    def _is_worker_running(self):

        # no lock file
        if not os.path.exists(self.worker_lock_file):
            return False

        # opening the lock file and to check if the pid is active
        lock_file = open(self.worker_lock_file, 'r')
        pid = BackgroundTaskRunner._format_int(lock_file.read())

        try:
            os.kill(pid, 0)
        except OSError:
            return False

        return True

    # creating lock file and storing our pid there
    def _start_worker(self):

        lock_file = open(self.worker_lock_file, 'w')
        lock_file.write(str(os.getpid()))
        lock_file.close()

    # --------------------------------------------------
    # UTILS
    # --------------------------------------------------

    @staticmethod
    def is_task_class_is_active(task_class_string):

        if task_class_string not in get_task_class_list():
            return False

        return True

    @staticmethod
    def get_worker_number(worker_argument):
        number_split = worker_argument.split('worker_')

        if len(number_split) != 2:
            return 0

        return BackgroundTaskRunner._format_int(number_split[1])

    # --------------------------------------------------
    # PROTECTED UTILS
    # --------------------------------------------------

    def _log(self, message, level='info'):

        message = "%s (%s): " % (self.task_class_string, str(self.worker_number)) + message

        if level == 'info':
            print("INFO: %s" % message)
            logging.getLogger('background_task').info(message)
        elif level == 'debug':
            print("DEBUG: %s" % message)
            logging.getLogger('background_task').debug(message)
        elif level == 'error':
            print("!!! ERROR !!!: %s" % message)
            logging.getLogger('background_task').error(message)

    # --------------------------------------------------
    # PROTECTED UTILS STATIC
    # --------------------------------------------------

    @staticmethod
    def _format_int(data_int):

        data_int = re.sub(",", '.', str(data_int).strip())
        data_int = re.sub("#[^0-9\.-]*#ism", "", data_int)

        try:
            data_int = int(data_int)
        except ValueError:
            return 0

        return data_int


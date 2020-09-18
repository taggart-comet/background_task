import os, time
import traceback
from sys import executable
from django.conf import settings
from django.db import models
from django.forms.models import model_to_dict
from django.utils.module_loading import import_string
from abc import ABC, abstractmethod
from BackgroundTask.logger import BackgroundTaskLogger

SCRIPT_PATH = executable + " " + str(settings.BASE_DIR) + "/manage.py"
RUN_SCRIPT_PATH = SCRIPT_PATH + " background_task_run"

# run script
CRONTAB_SH_PATH = os.path.dirname(os.path.abspath(__file__)) + "/crontab.sh"
CRONTAB_RUN = "* * * * * /bin/sh " + CRONTAB_SH_PATH + " >/dev/null 2>&1"
CRONTAB_CHECK_RUN_PATTERN = ""

# logrotate
CRONTAB_LOGROTATE = "0 3 * * * " + SCRIPT_PATH + " background_task_monitor --logrotate"
CRONTAB_CHECK_LOGROTATE_PATTERN = ""

# queue check
CRONTAB_QUEUE_CHECK = "*/15 * * * * " + SCRIPT_PATH + " background_task_monitor --check"
CRONTAB_CHECK_QUEUE_CHECK_PATTERN = ""


def get_task_class_list():
    return settings.BACKGROUND_TASK_CLASSES


def get_task_class_instance(task_class_string):
    task_class = import_string(task_class_string)
    return task_class()


def get_task_class_db_model(module, db_app_label, table_name) -> models.Model:
    class Meta:
        pass

    setattr(Meta, 'app_label', db_app_label)
    setattr(Meta, 'db_table', table_name)

    # Set up a dictionary to simulate declarations within a class
    attrs = {
        '__module__': module,
        'Meta': Meta,
        'task_id': models.AutoField(primary_key=True),
        'need_work': models.IntegerField(default=0),
        'errors': models.IntegerField(default=0),
        'date_added': models.IntegerField(default=0),
        'data_json': models.JSONField(),
    }

    # Create the class, which automatically triggers ModelBase processing
    model = type('BackgroundTaskGeneralModel', (models.Model,), attrs)

    return model


"""
Class executor.
Extends BackgroundTaskInterface and contains all functions
with sql requests to work with queue tasks
used in BackgroundTaskRunner
"""


class BackgroundTaskHandler(ABC):
    # --------------------------------------------------
    # USER SETTINGS
    # --------------------------------------------------

    # basic customization
    table_name: str = None
    db_app_label = 'default'
    worker_count = 1
    busy_interval = 1
    empty_interval = 10
    persistent_queue_on = False

    #
    task_limit_per_execution = 2
    task_execution_time = 600

    # logs and stats
    logs_on = True
    logs_db_app_name = 'default'
    logs_table_name: str = None
    logs_rotate_older: int = 30 # days

    # error handling
    retry_count_max = 3
    overflow_alarm = 10
    need_check_overflow = True

    # --------------------------------------------------
    # SERVICE ATTRIBUTES
    # --------------------------------------------------

    _task_id: int = None
    _db_model_instance: models.Model = None
    _logger_instance: BackgroundTaskLogger = None

    # --------------------------------------------------
    # MAIN
    # --------------------------------------------------

    @abstractmethod
    def work(self, data):
        pass

    """ This method is wrapping work(), and controlling the flow"""

    def do_work(self, task_row):

        self._task_id = task_row['task_id']

        self.logger().worker().info(
            "Starting to work on task: [%d], attempt: [%d]" % (task_row['task_id'], task_row['errors']+1),
            {'task_data': task_row['data_json']}
        )

        # first checking how many attempts this task has, should we even do it
        if task_row['errors'] > self.retry_count_max:
            self._on_task_complete(False, "Tried and failed for [%d] times" % task_row['errors'])
            return

        # working..
        try:
            result = self.work(task_row['data_json'])
        except Exception as e:

            self.logger().worker().error("An exception happened during the task execution. Traceback will be attached.")
            self._on_task_complete(False, traceback.format_exc())
            return

        if not result:
            self.logger().worker().error("The task returned False as a result. It will be retried")
            self.on_retry()
            return

        self._on_task_complete(True, "The task was completed successfully.")
        return

    def get_new_task_list(self, worker_number):

        # limit for performance, offset insures workers dont overlap
        limit = self.task_limit_per_execution
        offset = worker_number * limit

        # select tasks
        try:
            task_list = self._get_db_model().objects.filter(need_work__lte=int(time.time()))[offset:limit].all()
        except self._get_db_model().DoesNotExist:
            return []

        # making it array with dict-s for clarity
        result_task_list = []
        for row in task_list:
            result_task_list.append(model_to_dict(row))
            row.need_work = int(time.time()) + self.task_execution_time  # setting need_work here
            row.errors = row.errors + 1

        # updating need_work so tasks become unavailable for sometime, in which they will hopefully
        # be completed and deleted from the table
        self._get_db_model().objects.bulk_update(task_list, ['need_work', 'errors'], batch_size=3)

        return result_task_list

    # for adding tasks from anywhere in your code (usually from views)
    # one quick sql insert will be made
    # be conscious about putting lots of data into data, it can impact the amount of memory workers will occupy
    # need_work = is timestamp as int(time.time()) when the task should be run, runs immediately by default
    def add_task(self, data_dict: dict, need_work: int = 0):

        model = self._get_db_model()
        model_inst = model(
            need_work=need_work,
            date_added=int(time.time()),
            data_json=data_dict
        )
        model_inst.save()

    # through it we write logs into the logs table, if it's on
    def logger(self) -> BackgroundTaskLogger:

        if self._logger_instance is not None:
            return self._logger_instance

        # controlling if we need logs
        if self.logs_on:
            self._logger_instance = BackgroundTaskLogger(self.logs_table_name, self.logs_db_app_name)
        else:
            from unittest.mock import Mock
            self._logger_instance = Mock()

        return self._logger_instance

    #
    def get_queue_size(self):

        # select tasks
        try:
            tasks_count = self._get_db_model().objects.count()
        except self._get_db_model().DoesNotExist:
            return 0

        return tasks_count

    # --------------------------------------------------
    # USER-CUSTOM MISCELLANEOUS
    # --------------------------------------------------

    """ this method is called when `queue_alarm_size` is exceeded """
    def on_overflow(self):
        # you can, for example, send slack notifications here
        pass

    """ for statistics (or anything) """
    def on_ok(self):
        # increment you stats for success of the task
        pass

    def on_fail(self):
        # increment you stats for failure of the task
        pass

    def on_retry(self):
        # increment you stats for retry attempts
        pass

    # --------------------------------------------------
    # PROTECTED MAIN
    # --------------------------------------------------

    def _on_task_complete(self, is_success: bool, message = None):

        if is_success:
            if message is not None:
                self.logger().worker().success(message)
            self.on_ok()
        else:
            if message is not None:
                self.logger().worker().error(message)
            self.on_fail()


        # in this mode we keep the tasks running forever
        if self.persistent_queue_on:
            self.logger().save()
            return

        self.logger().worker().info("Deleting the task from the queue table.")
        self._delete_task()
        self.logger().save()

    def _delete_task(self):
        self._get_db_model().objects.filter(pk=self._task_id).delete()

    # --------------------------------------------------
    # PROTECTED UTILS
    # --------------------------------------------------

    def _get_db_model(self) -> models.Model:

        if self._db_model_instance is not None:
            return self._db_model_instance

        #
        self._db_model_instance = get_task_class_db_model(self.__module__, self.db_app_label, self.table_name)

        return self._db_model_instance

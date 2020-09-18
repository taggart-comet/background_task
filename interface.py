from abc import abstractmethod
from BackgroundTask.handler import BackgroundTaskHandler


""" Main interface class, that gets implemented by all task classes """
class BackgroundTaskInterface(BackgroundTaskHandler):
    # --------------------------------------------------
    # BASIC CUSTOMIZATION
    # --------------------------------------------------

    # queue table name for the task
    table_name:str = None

    # database where queue table will be created
    db_app_label:str = 'default'

    # number of workers to run
    worker_count:int = 1

    # sleep interval of the script when there are tasks in the queue (seconds)
    busy_interval:int = 1

    # sleep interval of the script when there are NO tasks in the queue (seconds)
    empty_interval:int = 10

    # when it False tasks are deleted from table on completion
    # if True, then they work forever with a period of `task_execution_time`
    persistent_queue_on = False

    # --------------------------------------------------
    # ETC
    # --------------------------------------------------

    # how many task to retrieve with one sql request to work in a loop
    # 1 is mostly okay, if the task is light and abundant (and the queue is high with =1) then can be increased up to 10
    task_limit_per_execution:int = 2

    # assumes that your single task doesn't take NO more than 10 min to complete
    # can be changed to anything, this also the time with which tasks will be
    # retried in case of a failure, if you decrease it less than the execution time - bad things will start to happen
    task_execution_time:int = 600

    # --------------------------------------------------
    # LOGS AND STATS
    # --------------------------------------------------

    logs_on = True

    # database where log table for the task will be created
    logs_db_app_name = 'default'

    # logs table will be name the same as `table_name` with logs_ prefix
    logs_table_name:str = None

    # how many logs to keep
    logs_rotate_older: int = 30  # days

    # --------------------------------------------------
    # ERROR HANDLING
    # --------------------------------------------------

    # how many times to retry after a failure (return False by work())
    retry_count_max:int = 3

    # if set to True, every 15 minutes a cron will be checking queue size
    # and if it's exceeds `overflow_alarm` it will fire `on_overflow()`
    need_check_overflow = True

    # max queue size to call the alarm
    overflow_alarm:int = 10

    def __init__(self):

        if self.table_name is None:
            self.table_name = str(self.__module__).lower().replace(".", "_")

        if self.logs_table_name is None:
            self.logs_table_name = 'logs_' + self.table_name

    # --------------------------------------------------
    # BASIC
    # --------------------------------------------------

    """ 
        the main method were the work is happening 
        data - is what you have put when created the task
        with .add_task(data) 
    """

    @abstractmethod
    def work(self, data):
        # here goes your background task code
        # return True to mark the task as successful
        # return False to mark the task as failed and retries it
        # `retry_count_max` times, set it to 0 if you don't need to
        pass

    # --------------------------------------------------
    # MISCELLANEOUS
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

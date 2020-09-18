import time
from django.core.management.base import BaseCommand, CommandError
from BackgroundTask import handler

class Command(BaseCommand):
    help = "Used by crontab, to to check queues and rotate logs."

    def add_arguments(self, parser):
        parser.add_argument(
            '--check',
            action='store_true',
            help="Check queue sizes. Checks every 15 minutes"
            ,
        )

        parser.add_argument(
            '--logrotate',
            action='store_true',
            help='Delete logs that are older then set. Works once a day',
        )

    def handle(self, *args, **options):

        if 'check' in options and options['check']:
            return self._check_queues()

        if 'logrotate' in options and options['logrotate']:
            return self._logrotate()

        self.stdout.write(self.style.WARNING(self.help))

    # --------------------------------------------------
    # PROTECTED
    # --------------------------------------------------

    def _check_queues(self):

        # getting all user's task classes
        class_list = handler.get_task_class_list()

        self.stdout.write("Checking queues..")
        for task_class_str in class_list:
            task_class_instance = handler.get_task_class_instance(task_class_str)

            if not task_class_instance.need_check_overflow:
                del task_class_instance
                continue

            queue_size = task_class_instance.get_queue_size()

            self.stdout.write("For [%s] queue is [%s]." % (task_class_str, str(queue_size)))

            if queue_size > task_class_instance.overflow_alarm:
                self.stdout.write(self.style.WARNING("QUEUE IS OVERFLOWN: ") + "Running alarm function..")
                task_class_instance.on_overflow()

            del task_class_instance

        self.stdout.write("[END]")

    def _logrotate(self):

        # getting all user's task classes
        class_list = handler.get_task_class_list()

        self.stdout.write("Checking queues..")
        for task_class_str in class_list:
            task_class_instance = handler.get_task_class_instance(task_class_str)

            if not task_class_instance.logs_on:
                continue

            older_than = int(time.time()) - (task_class_instance.logs_rotate_older * 86400)

            #
            self.stdout.write(self.style.WARNING("DELETING LOGS: ") + "[%s] older than: " % task_class_str + self.style.ERROR(time.strftime("%d %b %Y", time.localtime(older_than))))
            task_class_instance.logger().delete_logs_older_than(older_than)

        self.stdout.write("[END]")


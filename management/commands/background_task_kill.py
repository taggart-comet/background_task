from django.core.management.base import BaseCommand, CommandError
from BackgroundTask import handler
from BackgroundTask.run import BackgroundTaskRunner



class Command(BaseCommand):
    help = "Kills running workers. Use cases: (1) reload workers to update work() code, (2) stop the queue after deleting it from settings.BACKGROUND_TASK_CLASSES"

    def add_arguments(self, parser):
        parser.add_argument(
            'class_name',
            help="Type `all` to reload all active queues"
        )

    def handle(self, *args, **options):

        # by class_name
        if options['class_name'] != 'all':
            # to determine how many workers are needed to be slaughtered
            task_class_inst = handler.get_task_class_instance(options['class_name'])

            self.stdout.write(self.style.WARNING(
                "PROGRESS: ") + options['class_name'] + ": killing.. %d workers" % task_class_inst.worker_count)
            for worker_number in range(task_class_inst.worker_count):
                runner = BackgroundTaskRunner(options['class_name'], worker_number)
                runner.stop()

            self.stdout.write(self.style.SUCCESS("END"))
            return

        # for all
        class_list = handler.get_task_class_list()

        for task_class_str in class_list:

            # to determine how many workers are needed to be slaughtered
            task_class_inst = handler.get_task_class_instance(task_class_str)

            self.stdout.write(self.style.WARNING("PROGRESS: ") + task_class_str + ": killing.. %d workers" % task_class_inst.worker_count)
            for worker_number in range(task_class_inst.worker_count):
                runner = BackgroundTaskRunner(task_class_str, worker_number)
                runner.stop()
                del runner

        self.stdout.write(self.style.SUCCESS("END"))


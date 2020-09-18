from django.core.management.base import BaseCommand, CommandError
from BackgroundTask.run import BackgroundTaskRunner


class Command(BaseCommand):
    help = "Used by crontab, to run task queues. Can also be used manually to test your code in `work()`."

    def add_arguments(self, parser):
        parser.add_argument(
            'class_name',
        )
        parser.add_argument(
            'worker',
        )

    def handle(self, *args, **options):

        if not BackgroundTaskRunner.is_task_class_is_active(options['class_name']):
            self.stdout.write(self.style.ERROR('ERROR!') + ' Class %s is not active!' % options['class_name'])
            return

        run = BackgroundTaskRunner(options['class_name'], BackgroundTaskRunner.get_worker_number(options['worker']))

        return run.run()

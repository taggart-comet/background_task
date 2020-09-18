import subprocess, re, os
from django.core.management.base import BaseCommand, CommandError
from django.db.utils import ProgrammingError, OperationalError
from BackgroundTask import handler
from BackgroundTask import logger


class Command(BaseCommand):
    help = "Background task management commands. First run --migrate, then --activate and you're good to go"

    def add_arguments(self, parser):
        parser.add_argument(
            '--migrate',
            action='store_true',
            help="To create tables for queues, after creating new task classes. If you need to rename/change table name, you will have to delete it manually and run --migrate again"
            ,
        )

        parser.add_argument(
            '--activate',
            action='store_true',
            help='To create start new task queues, after creating new task classes and migrating',
        )

    def handle(self, *args, **options):

        if 'migrate' in options and options['migrate']:
            return self._migrate()

        if 'activate' in options and options['activate']:
            return self._activate()

        self.stdout.write(self.style.WARNING(self.help))

    # --------------------------------------------------
    # PROTECTED
    # --------------------------------------------------

    def _migrate(self):

        # getting all user's task classes
        class_list = handler.get_task_class_list()

        self.stdout.write("Checking tables..")
        for task_class_str in class_list:
            task_class_instance = handler.get_task_class_instance(task_class_str)

            # --------------------------------------------------
            # queue table
            # --------------------------------------------------
            db_model = handler.get_task_class_db_model(task_class_instance.__module__, task_class_instance.db_app_label, task_class_instance.table_name)

            # checking if a table for the model exists
            try:
                exists = self._if_table_exists(db_model)
            except AttributeError:
                self.stdout.write(self.style.ERROR(
                    "ERROR: ") + task_class_str + ": table structure was changed. Please delete the table and run again.")
                return

            if not exists:
                # creating the table
                if self._create_table(task_class_instance.db_app_label, db_model):
                    self.stdout.write(self.style.SUCCESS("OK: ") + task_class_str + ": [queue] table created")

            # --------------------------------------------------
            # logs table
            # --------------------------------------------------


            if task_class_instance.logs_on:

                logs_db_model = logger.get_logs_class_db_model(task_class_instance.__module__, task_class_instance.logs_db_app_name, task_class_instance.logs_table_name)

                # checking if a table exists
                try:
                    logs_exists = self._if_table_exists(logs_db_model)
                except AttributeError:
                    self.stdout.write(self.style.ERROR(
                        "ERROR: ") + task_class_str + ": table structure was changed. Please delete the table and run again.")
                    return

                if not logs_exists:
                    # creating the table for logs
                    if self._create_table(task_class_instance.logs_db_app_name, logs_db_model):
                        self.stdout.write(self.style.SUCCESS("OK: ") + task_class_str + ": [logs] table created")

        self.stdout.write(self.style.SUCCESS("[Migration is done]"))

    def _activate(self):

        # getting all user's task classes
        class_list = handler.get_task_class_list()
        header = handler.RUN_SCRIPT_PATH + " "

        # WARNING this may not work everywhere, but I'll keep it this way for now
        crontab_sh = "#!/bin/bash\n"

        # re generating crontab file
        for task_class_str in class_list:

            # determining how many workers are needed
            task_class_inst = handler.get_task_class_instance(task_class_str)

            self.stdout.write(self.style.WARNING("PROGRESS: ") + task_class_str + ": checking.. %d workers needed" % task_class_inst.worker_count)

            crontab_sh = crontab_sh + "\n##### Workers of " + task_class_str + "\n"
            for i in range(task_class_inst.worker_count):
                crontab_sh = crontab_sh + header + task_class_str + ' worker_' + str(i) + ";\n"

            crontab_sh = crontab_sh + "\n"

        # writing into the crontab init file
        f = open(handler.CRONTAB_SH_PATH, 'w')
        f.write(crontab_sh)
        f.close()

        #
        self._check_and_update_crontab()

        self.stdout.write(self.style.SUCCESS("Activated!"))

    # --------------------------------------------------
    # UTILS
    # --------------------------------------------------

    def _create_table(self, db_app_label, model):
        from django.db import connections

        with connections[db_app_label].schema_editor() as schema_editor:
            schema_editor.create_model(model)

        return True

    def _if_table_exists(self, model):
        try:
            model.objects.filter(pk=1)[0:1].get()
        except ProgrammingError:
            return False
        except OperationalError:
            raise AttributeError
        except model.DoesNotExist:
            return True

        return True

    def _check_and_update_crontab(self):

        # run
        if not self._is_crontab_has_line(handler.CRONTAB_RUN):
            self.stdout.write(self.style.WARNING("PROGRESS: ") + "Updating crontab for run.. ")
            self._add_row_to_crontab(handler.CRONTAB_RUN)

        # check queue
        if not self._is_crontab_has_line(handler.CRONTAB_QUEUE_CHECK):
            self.stdout.write(self.style.WARNING("PROGRESS: ") + "Updating crontab for check queue.. ")
            self._add_row_to_crontab(handler.CRONTAB_QUEUE_CHECK)

        # logrotate
        if not self._is_crontab_has_line(handler.CRONTAB_LOGROTATE):
            self.stdout.write(self.style.WARNING("PROGRESS: ") + "Updating crontab for logrotate.. ")
            self._add_row_to_crontab(handler.CRONTAB_LOGROTATE)


    def _is_crontab_has_line(self, target_line):

        # checking that crontab file is included in crontab
        current_cron_args = ['crontab', '-l']
        try:
            current_cron = subprocess.Popen(current_cron_args, stdout=subprocess.PIPE)
            current_cron.wait()
        except OSError:
            self.stdout.write(self.style.ERROR(
                "ERROR: ") + "Check that crontab is install on your system and accessible for the current user")
            return False

        current_cron_str = current_cron.stdout.read()

        for line in iter(current_cron_str.splitlines()):
            if target_line == line.decode():
                return True

        return False

    def _add_row_to_crontab(self, crontab_row):

        crontab_row = crontab_row + "\n"

        try:
            update_cron = subprocess.Popen("crontab -l | { cat; echo \"" + crontab_row + "\"; } | crontab -",
                                           stdout=subprocess.PIPE, shell=True)
            update_cron.wait()
        except OSError:
            self.stdout.write(self.style.ERROR(
                "ERROR: ") + "Something went wrong while setting crontab..")
            return False

        if update_cron.returncode != 0:
            self.stdout.write(self.style.ERROR(
                "ERROR: ") + "Something went wrong while setting crontab...")
            return False

        return True

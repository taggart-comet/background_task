#!/bin/bash

##### Workers of crons.system.email.CronSystemEmail
/usr/bin/python /home/share/ComponentDesigner/backend/manage.py background_task_run crons.system.email.CronSystemEmail worker_0;


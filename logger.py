import re
import time
from django.db import models
from django.forms.models import model_to_dict

def get_logs_class_db_model(module, db_app_label, table_name) -> models.Model:

    class Meta:
        pass

    setattr(Meta, 'app_label', db_app_label)
    setattr(Meta, 'db_table', table_name)

    # Set up a dictionary to simulate declarations within a class
    attrs = {
        '__module__': module,
        'Meta': Meta,
        'id': models.AutoField(primary_key=True),
        'date_added': models.IntegerField(default=0),
        'message_json': models.JSONField(),
    }

    # Create the class, which automatically triggers ModelBase processing
    model = type('BackgroundTaskGeneralLogsModel', (models.Model,), attrs)

    return model

class BackgroundTaskLogger:

    #
    logs_db_app_name = 'default'
    logs_table_name: str = None

    #
    _message_source = 'worker' # worker or user
    _MESSAGE_SOURCE_WORKER = 0
    _MESSAGE_SOURCE_USER = 1

    #
    _MESSAGE_TYPE_INFO = 0
    _MESSAGE_TYPE_SUCCESS = 1
    _MESSAGE_TYPE_ERROR = 2

    #
    _db_model_inst: models.Model = None
    _message_structure: [dict] = []

    def __init__(self, logs_table_name, logs_db_app_name = 'default'):
        self.logs_table_name = logs_table_name
        self.logs_db_app_name = logs_db_app_name

    # --------------------------------------------------
    # Writing logs interface
    # --------------------------------------------------

    def user(self):
        self._message_source = self._MESSAGE_SOURCE_USER
        return self

    def worker(self):
        self._message_source = self._MESSAGE_SOURCE_USER
        return self

    def info(self, message_text, data = None):
        return self._log(message_text, self._MESSAGE_TYPE_INFO, data)

    def error(self, message_text, data = None):
        return self._log(message_text, self._MESSAGE_TYPE_ERROR, data)

    def success(self, message_text, data = None):
        return self._log(message_text, self._MESSAGE_TYPE_SUCCESS, data)

    def save(self):

        model = self._get_db_model()
        model_inst = model(
            date_added=int(time.time()),
            message_json=self._message_structure
        )
        model_inst.save()

        self._message_structure = []

    # --------------------------------------------------
    # Retrieving logs interface
    # --------------------------------------------------

    def get_last_logs(self, limit = 20, offset = 0):
        logs_list = self._get_db_model().objects.order_by('-id')[offset:offset+limit].all()

        # making it array with dict-s for clarity
        result_list = []
        for row in logs_list:
            result_list.append(model_to_dict(row))

        return result_list

    # --------------------------------------------------
    # LOGROTATE
    # --------------------------------------------------

    def delete_logs_older_than(self, timestamp):
        self._get_db_model().objects.filter(date_added__lte=timestamp).delete()

    # --------------------------------------------------
    # PROTECTED
    # --------------------------------------------------

    def _log(self, message_text, message_type, data = None):

        # forming the message dictionary
        # appending to the structure
        if data is None:
            data = {}
        self._message_structure.append({
            'date_added': int(time.time()),
            'message_type': message_type,
            'message_source': self._message_source,
            'message_text': self._format_string(message_text),
            'extra_data': data
        })

    # --------------------------------------------------
    # PROTECTED UTILS
    # --------------------------------------------------

    def _get_db_model(self) -> models.Model:
        if self._db_model_inst is not None:
            return self._db_model_inst

        #
        self._db_model_inst = get_logs_class_db_model(self.__module__, self.logs_db_app_name, self.logs_table_name)

        return self._db_model_inst

    def _format_string(self, data_string):
        regex = """
            /
  (
    (?: [\x00-\x7F]                 # single-byte sequences   0xxxxxxx
    |   [\xC0-\xDF][\x80-\xBF]      # double-byte sequences   110xxxxx 10xxxxxx
    |   [\xE0-\xEF][\x80-\xBF]{2}   # triple-byte sequences   1110xxxx 10xxxxxx * 2
    |   [\xF0-\xF7][\x80-\xBF]{3}   # quadruple-byte sequence 11110xxx 10xxxxxx * 3
    ){1,100}                        # ...one or more times
  )
| .                                 # anything else
/x
            """
        return re.sub(regex, '$1', data_string.strip())
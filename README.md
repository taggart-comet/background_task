**_Light, high-load ready, handy background-task module for Django._**

##### Use case #1:
>I need to send emails to my users asynchronously.

STEP 1: Write a task class:
>Wherever in your code you feel is right, create a class:
```
from BackgroundTask.interface import BackgroundTaskInterface
from django.core.mail import BadHeaderError, send_mail


class QueueSendEmail(BackgroundTaskInterface):

    def work(self, data):

        try:
            send_mail(
                data['subject'],
                data['message'],
                'from@example.com',
                [data['email']],
                fail_silently=False,
            )
        except BadHeaderError:
            return False
        
        return True

```

STEP 2: Add this class to settings.py:
```
BACKGROUND_TASK_CLASSES = [
    'mypath.QueueSendEmail'
]
```

STEP 3: Create tables for queues and logs:
```
./manage.py background_task --migrate
```

STEP 4: Launch the queue:
```
./manage.py background_task --activate
```

STEP 5: Add tasks to the queue
```
from ApiHandler.response import ApiResponse
from django.views.decorators.http import require_POST
from mypath.email import QueueSendEmail

@require_POST
def send_email_code(request):

    email = PostDataValidator.email(request.post_data, 'email')

    # ...

    # adding task to send the email
    QueueSendEmail().add_task({
        'email': email,
        'subject': 'Login to myawesomesite.com',
        'message': 'Your login code is: %s' % code
    })

    # ...

    return ApiResponse.ok()

```

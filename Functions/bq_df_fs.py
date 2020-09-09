import base64
import os
import time
from googleapiclient.discovery import build


def hello_pubsub(event, context):
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    print(pubsub_message)

    project = os.environ.get('PROJECT')
    template = 'gs://yelp_help_dataflow/dataflow_templates/BQtoDF'
    job = "yelp-help-review-" + str(time.time_ns())

    dataflow = build('dataflow', 'v1b3')
    request = dataflow.projects().templates().launch(projectId=project, gcsPath=template, body={'jobName': job})

    response = request.execute()

    return response

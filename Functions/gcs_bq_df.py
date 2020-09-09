from google.cloud import bigquery
import os
import logging

PROJECT_NAME = os.environ.get('PROJECT')
DATASET_NAME = os.environ.get('DATASET')
TABLE_NAME = os.environ.get('TABLE')


def batch_ingest(event, context):

    client = bigquery.Client()

    BUCKET_NAME = event['bucket']
    print(f"Processing bucket: {BUCKET_NAME}.")

    FILE_NAME = event['name']
    print(f"Processing file: {FILE_NAME}.")

    timeCreated = event['timeCreated']
    print(f"Time created: {timeCreated}.")

    table_id = '%s.%s.%s' % (PROJECT_NAME, DATASET_NAME, TABLE_NAME)

    dataset_ref = client.dataset(DATASET_NAME)

    uri = 'gs://%s/%s' % (BUCKET_NAME, FILE_NAME)


    try:
        client.get_dataset(dataset_ref)
    except Exception:
        logging.warning('Creating dataset: %s' % (DATASET_NAME))
        client.create_dataset(dataset_ref)

    # create a bigquery load job config
    job_config = bigquery.LoadJobConfig()
    job_config.autodetect = True
    job_config.create_disposition = 'CREATE_IF_NEEDED',
    job_config.source_format = 'NEWLINE_DELIMITED_JSON',
    job_config.write_disposition = 'WRITE_APPEND',

    # create a bigquery load job
    try:
        load_job = client.load_table_from_uri(
            uri,
            table_id,
            job_config=job_config,
        )
        print('Load job: %s [%s]' % (
            load_job.job_id,
            table_id
        ))
    except Exception as e:
        logging.error('Failed to create load job: %s' % e)

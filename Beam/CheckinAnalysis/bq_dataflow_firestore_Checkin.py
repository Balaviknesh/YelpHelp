import apache_beam as beam
from apache_beam.io import BigQuerySource
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions
import logging
from google.cloud import firestore
from apache_beam.options.pipeline_options import PipelineOptions


PROJECT = 'yelphelp'
BUCKET = 'yelp_help_dataflow'

options = PipelineOptions()
google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = PROJECT
google_cloud_options.staging_location = "gs://"+BUCKET+"/staging"
google_cloud_options.temp_location = "gs://"+BUCKET+"/temp"
options.view_as(StandardOptions).runner = "DataFlowRunner"

source = BigQuerySource(query="""SELECT business_id, count(checkin) as checkins FROM `yelphelp.YearlyData.Checkin` group by business_id""",
                        use_standard_sql=True)


class CreateEntities(beam.DoFn):

    def process(self, element):
        document_id = str(element.pop('business_id'))
        element['checkins'] = str(element['checkins'])
        return [(document_id, element)]


class FirestoreWriteDoFn(beam.DoFn):
    MAX_DOCUMENTS = 200

    def start_bundle(self):
        self._records = []

    def finish_bundle(self):
        if self._records:
            self._flush_batch()

    def process(self, element, *args, **kwargs):
        self._records.append(element)
        if len(self._records) > self.MAX_DOCUMENTS:
            self._flush_batch()

    def _flush_batch(self):
        db = firestore.Client(project="yelphelp")
        batch = db.batch()
        for record in self._records:
            ref = db.collection("business").document(record[0])
            batch.set(ref, record[1], merge=True)
        r = batch.commit()
        logging.debug(r)
        self._records = []


def run():
    with beam.Pipeline(options=options) as p:
        (p
         | "ReadTable" >> beam.io.Read(source)
         | 'Create entities' >> beam.ParDo(CreateEntities())
         | 'Write entities into Firestore' >> beam.ParDo(FirestoreWriteDoFn())
         )


if __name__ == '__main__':
    run()

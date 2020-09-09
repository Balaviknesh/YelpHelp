import apache_beam as beam
from apache_beam.io import BigQuerySource
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions
import logging
from google.cloud import firestore
from apache_beam.options.pipeline_options import PipelineOptions
import datetime

PROJECT = 'yelphelp'
BUCKET = 'yelp_help_dataflow'


options = PipelineOptions()
google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = PROJECT
google_cloud_options.staging_location = "gs://"+BUCKET+"/staging"
google_cloud_options.temp_location = "gs://"+BUCKET+"/temp"
options.view_as(StandardOptions).runner = "DataFlowRunner"

source = BigQuerySource(query="""SELECT R.business_id, b.name, b.categories, b.city, b.state, sum(R.useful) as useful,\
 sum(R.funny) as funny, sum(R.cool) as cool, avg(R.stars) as avg_stars, count(R.stars) as num_stars \
 FROM `yelphelp.YearlyData.Review` as R LEFT JOIN `yelphelp.YearlyData.Business` as B ON \
  R.business_id = B.business_id group by business_id, name, categories, state, city""",
                        use_standard_sql=True)


class CreateEntities(beam.DoFn):

    def process(self, element):
        document_id = str(element.pop('business_id'))
        element['name'] = str(element['name'])
        element['categories'] = str(element['categories'])
        element['city'] = str(element['city'])
        element['state'] = str(element['state'])
        element['cool'] = int(element['cool'])
        element['funny'] = int(element['funny'])
        element['useful'] = int(element['useful'])
        element['avg_stars'] = float(element['avg_stars'])
        element['num_stars'] = int(element['num_stars'])
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

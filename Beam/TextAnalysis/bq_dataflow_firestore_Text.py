import apache_beam as beam
from apache_beam.io import BigQuerySource
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions
import logging
from google.cloud import firestore
from apache_beam.options.pipeline_options import PipelineOptions
from collections import Counter
from nltk import WordNetLemmatizer

stop_words = {'i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 'you', "you're", "you've", "you'll", "you'd",
              'your', 'yours', 'yourself', 'yourselves', 'he', 'him', 'his', 'himself', 'she', "she's", 'her', 'hers',
              'herself', 'it', "it's", 'its', 'itself', 'they', 'them', 'their', 'theirs', 'themselves', 'what',
              'which', 'who', 'whom', 'this', 'that', "that'll", 'these', 'those', 'am', 'is', 'are', 'was', 'were',
              'be', 'been', 'being', 'have', 'has', 'had', 'having', 'do', 'does', 'did', 'doing', 'a', 'an', 'the',
              'and', 'but', 'if', 'or', 'because', 'as', 'until', 'while', 'of', 'at', 'by', 'for', 'with', 'about',
              'against', 'between', 'into', 'through', 'during', 'before', 'after', 'above', 'below', 'to', 'from',
              'up', 'down', 'in', 'out', 'on', 'off', 'over', 'under', 'again', 'further', 'then', 'once', 'here',
              'there', 'when', 'where', 'why', 'how', 'all', 'any', 'both', 'each', 'few', 'more', 'most', 'other',
              'some', 'such', 'no', 'nor', 'not', 'only', 'own', 'same', 'so', 'than', 'too', 'very', 's', 't', 'can',
              'will', 'just', 'don', "don't", 'should', "should've", 'now', 'd', 'll', 'm', 'o', 're', 've', 'y', 'ain',
              'aren', "aren't", 'couldn', "couldn't", 'didn', "didn't", 'doesn', "doesn't", 'hadn', "hadn't", 'hasn',
              "hasn't", 'haven', "haven't", 'isn', "isn't", 'ma', 'mightn', "mightn't", 'mustn', "mustn't", 'needn',
              "needn't", 'shan', "shan't", 'shouldn', "shouldn't", 'wasn', "wasn't", 'weren', "weren't", 'won', "won't",
              'wouldn', "wouldn't"}

PROJECT = 'yelphelp'
BUCKET = 'yelp_help_dataflow'

options = PipelineOptions()
google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = PROJECT
google_cloud_options.staging_location = "gs://" + BUCKET + "/staging"
google_cloud_options.temp_location = "gs://" + BUCKET + "/temp"
options.view_as(StandardOptions).runner = "DataFlowRunner"

source = BigQuerySource(query="""SELECT A.business_id as business_id,  regexp_replace(lower(STRING_AGG(A.text)), '[^a-zA-Z0-9 ]', '') as comments FROM `yelphelp.YearlyData.Review` as A GROUP BY business_id""", use_standard_sql=True)


def RemoveStopWords(element):
    word_tokens = str(element['comments']).split()
    element['comments'] = [w for w in word_tokens if w not in stop_words]
    return element


def Lemmatization(element):
    lemmatizer = WordNetLemmatizer()
    element['comments'] = [lemmatizer.lemmatize(w) for w in element['comments']]
    return element


def Top5MostFrequentWords(element):
    element['comments'] = ' '.join([word[0] for word in Counter(element['comments']).most_common(5)])
    return element


class CreateEntities(beam.DoFn):

    def process(self, element, *args, **kwargs):
        document_id = str(element.pop('business_id'))
        element['comments'] = str(element['comments'])

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
         | "Tokenize and Remove Stop Words" >> beam.Map(RemoveStopWords)
         | "Lemmatization" >> beam.Map(Lemmatization)
         | "Top 5 Most Frequent Words" >> beam.Map(Top5MostFrequentWords)
         | 'Create entities' >> beam.ParDo(CreateEntities())
         | 'Write entities into Firestore' >> beam.ParDo(FirestoreWriteDoFn())
         )


if __name__ == '__main__':
    run()

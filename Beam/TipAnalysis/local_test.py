import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions
from collections import Counter
from textblob import TextBlob

options = PipelineOptions()

class Printer(beam.DoFn):
    def process(self, data_item):
        print(data_item)


class TypeOf(beam.DoFn):
    def process(self, data_item):
        print(type(data_item))


def run():
    with beam.Pipeline(options=options) as p:
        (p
         | "READ" >> ReadFromText("results-20200908-184206.json")
         | "Splitter using beam.Map" >> beam.Map(lambda record: (record.split(',')))
         | "Print" >> beam.ParDo(Printer())
         )


if __name__ == '__main__':
    run()

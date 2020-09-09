import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions
from collections import Counter
from textblob import TextBlob

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

options = PipelineOptions()

def RemoveStopWords(element):
    word_tokens = str(element[1]).split()
    element[1] = [w for w in word_tokens if w not in stop_words]
    return element


def Lemmatization(element):
    element[1] = lemmatize_with_postag(element[1])
    return element


def Top5MostFrequentWords(element):
    element[1] = ' '.join([word[0] for word in Counter(element[1]).most_common(5)])
    return element


def lemmatize_with_postag(sentence):
    sent = TextBlob(sentence)
    tag_dict = {"J": 'a',
                "N": 'n',
                "V": 'v',
                "R": 'r'}
    words_and_tags = [(w, tag_dict.get(pos[0], 'n')) for w, pos in sent.tags]
    lemmatized_list = [wd.lemmatize(tag) for wd, tag in words_and_tags]
    return " ".join(lemmatized_list)

class Printer(beam.DoFn):
    def process(self, data_item):
        print(data_item)


class TypeOf(beam.DoFn):
    def process(self, data_item):
        print(type(data_item))


def run():
    with beam.Pipeline(options=options) as p:
        (p
         | "READ" >> ReadFromText("results-20200907-181405.csv")
         | "Splitter using beam.Map" >> beam.Map(lambda record: (record.split(',')))
         | "Lemmatization" >> beam.Map(Lemmatization)
         | "Tokenize and Remove Stop Words" >> beam.Map(RemoveStopWords)
         | "Top 5 Most Frequent Words" >> beam.Map(Top5MostFrequentWords)
         | "Print" >> beam.ParDo(Printer())
         )


if __name__ == '__main__':
    run()

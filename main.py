import apache_beam as beam
from apache_beam.pipeline import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from statistics import  mean
import logging
import argparse
import re

#inputfile="F:/Projects/Flipkart-mobile-prices/flipkart_mobiles.csv"
#outputfile="F:/Projects/Flipkart-mobile-prices/avgprice"


table_spec = 'flipkart.mobile_prices'
table_schema='Brand:STRING, Model:STRING, Color:STRING, Memory:STRING, Storage:STRING, Rating:FLOAT, Selling_Price:INT64, Original_Price:INT64'

class DataIngestion:
    def parse_method(self, string_input):
        values = re.split(",", re.sub('\r\n', '', re.sub('"', '',string_input)))
        row = dict(zip(('Brand', 'Model', 'Color', 'Memory', 'Storage', 'Rating','Selling_Price','Original_Price'),values))
        return row

class GreaterThanAvg(beam.DoFn):
    def process(self, element,side_input):
        avg=mean(side_input)

        if int(element[7]) > avg:
            yield element

def replace_space_with_zero(line):
  return line.replace(' ', '0')

def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument('--inputfile',dest='input', help='Input file to process.' ,required=True)
    parser.add_argument('--outputfile',dest='output', help='Input file to process.')

    known_args, pipline_args = parser.parse_known_args(argv)
    pipelineoptions = PipelineOptions(pipline_args)
    pipelineoptions.view_as(SetupOptions).save_main_session = save_main_session

    dataingestion = DataIngestion()

    with beam.Pipeline(options=pipelineoptions) as p:

        maindata=(
            p| "Reading file" >> beam.io.ReadFromText(known_args.input,skip_header_lines=1)
            |"Split" >> beam.Map(lambda x:x.split(","))
            )

        side_input=(
         maindata | "Side input pricing " >> beam.Map(lambda x:int(x[7]))
        )

        greater_than_avg=(
            maindata| "greater than avg" >> beam.ParDo(GreaterThanAvg(),beam.pvalue.AsList(side_input)) \
            | "join the data">> beam.Map(lambda x:','.join(x))  \
            | "DataIngetion " >> beam.Map(lambda s: dataingestion.parse_method(s)) \
            #| "write to file " >> beam.io.WriteToText(known_args.output)
            | "Write to BQ" >> beam.io.WriteToBigQuery(table_spec, schema=table_schema,
                                                      write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                                                       create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
            )


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
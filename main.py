import apache_beam as beam
from apache_beam.pipeline import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from statistics import  mean
import logging
import argparse


inputfile="F:/Projects/Flipkart-mobile-prices/flipkart_mobiles.csv"
outputfile="F:/Projects/Flipkart-mobile-prices/avgprice"

class PrintFn(beam.DoFn):
    def process(self, element):
        print(element)


class GreaterThanAvg(beam.DoFn):
    def process(self, element,side_input):
        avg=mean(side_input)

        if int(element[7]) > avg:
            yield element

def replace_space_with_zero(line):
  return line.replace(' ', '0')

def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument('--inputfile',dest='input', help='Input file to process.')
    parser.add_argument('--outputfile',dest='output', help='Input file to process.')

    known_args, pipline_args = parser.parse_known_args(argv)
    pipelineoptions = PipelineOptions(pipline_args)
    pipelineoptions.view_as(SetupOptions).save_main_session = save_main_session

    with beam.Pipeline() as p:

        maindata=(
            p| "Reading file" >> beam.io.ReadFromText(inputfile,skip_header_lines=1)   #read file data into pcollection maindata
            |"Split" >> beam.Map(lambda x:x.split(","))
            )
        Gettotalcount=(
            maindata | "total Count" >> beam.combiners.Count.Globally() |  'print-total' >> beam.ParDo(PrintFn())  #get total counts of record
        )
        side_input=(
         maindata | "Side input pricing " >> beam.Map(lambda x:int(x[7]))  # price column x[7] to calculate avg
        )

        greater_than_avg=(
            maindata| "Calculate avg values" >> beam.ParDo(GreaterThanAvg(),beam.pvalue.AsList(side_input)) #pass the sideinput to Pardo GreaterThanAvg\
            | beam.Map(lambda x:','.join(x)) \
            | beam.Map(replace_space_with_zero) \
            #| beam.Map(print)
            )
        Getavgcount = (
                greater_than_avg | "get avg Count" >> beam.combiners.Count.Globally() | 'print-avg' >> beam.ParDo(PrintFn()) #get avg value count
        )
        Write=(
                greater_than_avg | "write to file " >> beam.io.WriteToText(outputfile) #write to file
        )

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()

import apache_beam as beam
from apache_beam.coders.coders import Coder
from apache_beam.transforms import Filter
from statistics import  mean
import  logging


inputfile="F:/Projects/Flipkart-mobile-prices/flipkart_mobiles.csv"
outputfile="F:/Projects/Flipkart-mobile-prices/avgprice"


class GreaterThanAvg(beam.DoFn):
    def process(self, element,side_input):
        avg=mean(side_input)

        if int(element[7]) > avg:
            yield element

def run():
    with beam.Pipeline() as p:

        maindata=(
            p| "Reading file" >> beam.io.ReadFromText(inputfile,skip_header_lines=1)  \
            |"Split" >> beam.Map(lambda x:x.split(","))
            )

        side_input=(
         maindata | "Side input pricing " >> beam.Map(lambda x:int(x[7]))  # price column x[3] to calculate avg
        )

        greater_than_avg=(
            maindata| "Calculate avg values" >> beam.ParDo(GreaterThanAvg(),beam.pvalue.AsList(side_input)) #pass the sideinput to Pardo\
            | "write to file " >> beam.io.WriteToText(outputfile)
            )


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
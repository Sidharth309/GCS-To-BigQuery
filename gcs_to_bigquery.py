import argparse
import time
import logging
import json
import apache_beam as beam
from apache_beam.io import fileio
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.transforms.sql import SqlTransform
from apache_beam.runners import DataflowRunner, DirectRunner

class ConvertToDict(beam.DoFn):
    def process(self, element):
        Date,Open,High,Low,Close,Volume,OpenInt = element.split(',')
        json_conv = {"Date": Date.strip(), "Open": Open.strip(), "High": High.strip(), "Low": Low.strip(), "Close": Close.strip(), "Volume": Volume.strip(), "OpenInt": OpenInt.strip()}
        dict_conv = json.loads(json_conv.decode('utf-8'))
        if dict_conv["Date"] == "Date":
            yield beam.pvalue.TaggedOutput('incorrect_type', element)
        else:
            yield beam.pvalue.TaggedOutput('correct_type', element)

#main            
def run():
    #command line arguments
    parser = argparse.ArgumentParser(description='Load from GCS into Bigquery')
    
    #Google Cloud options
    parser.add_argument('--project',required=True, help='Specify Google Cloud project')
    parser.add_argument('--region', required=True, help='Specify Google Cloud region')
    parser.add_argument('--stagingLocation', required=True, help='Specify Cloud Storage bucket for staging')
    parser.add_argument('--tempLocation', required=True, help='Specify Cloud Storage bucket for temp')
    parser.add_argument('--runner', required=True, help='Specify Apache Beam Runner')

    #Beam pipeline options
    options = PipelineOptions()
    options.view_as(GoogleCloudOptions).project = opts.project
    options.view_as(GoogleCloudOptions).region = opts.region
    options.view_as(GoogleCloudOptions).staging_location = opts.stagingLocation
    options.view_as(GoogleCloudOptions).temp_location = opts.tempLocation
    options.view_as(GoogleCloudOptions).job_name = '{0}{1}'.format('my-pipeline-',time.time_ns())
    options.view_as(StandardOptions).runner = opts.runner

    #input and output
    input = 'gs://stock-market-dataset/archive'
    output = 'pro-talon-385304:stocks.stocks'
    dead_letter_output = 'gs://stock-market-dataset/deadletter/'

    table_schema = {
            "fields": [
                {
                    "name": "Date",
                    "type": "STRING"
                },
                {
                    "name": "Open",
                    "type": "FLOAT"
                },
                {
                    "name": "High",
                    "type": "FLOAT"
                },
                {
                    "name": "Low",
                    "type": "FLOAT"
                },
                {
                    "name": "Close",
                    "type": "FLOAT"
                },
                {
                    "name": "Volume",
                    "type": "INTEGER"
                },
                {
                    "name": "OpenInt",
                    "type": "INTEGER"
                }
            ]
        }
    
    #Table schema for BigQuery
    agg_table_schema = {
            "fields": [
                {
                    "name": "Year",
                    "type": "INTEGER"
                },
                {
                    "name": "Quarter",
                    "type": "INTEGER"
                },
                {
                    "name": "Market_Open_Days",
                    "type": "INTEGER"
                }
            ]
        }    
    
    query = """
        SELECT EXTRACT(YEAR FROM Date) AS Year,
        EXTRACT(QUARTER FROM Date) AS Quarter,
        COUNT(Date) AS Market_Open_Days
        FROM PCOLLECTION
        GROUP BY EXTRACT(YEAR FROM date), EXTRACT(QUARTER FROM date)
        ORDER BY EXTRACT(YEAR FROM date), EXTRACT(QUARTER FROM date)
        """

    #Pipline creation
    p = beam.Pipeline(options=options)

    (p
         | 'ReadFromGCS' >> beam.io.ReadFromText(input, skip_header_lines=1)
         | 'ConvertToDict' >> beam.ParDo(ConvertToDict()).with_outputs('correct_type', 'incorrect_type'))
    
    #Dead letter Queue
    (p.incorrect_type
         | 'DeadLetterStorage' >> fileio.WriteToFiles(dead_letter_output,
                                                      shards=1,
                                                      max_writers_per_bundle=0)
    )
    
    #Using SQL for analysis and writing to BigQuery for correct input types
    (p.correct_type
         | 'Query' >> SqlTransform(query, dialect = 'zetasql')
         | 'WriteToBQ' >> beam.io.WriteToBigQuery(
                output,
                schema=agg_table_schema,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
                )
    )

    logging.getLogger().setLevel(logging.INFO)
    logging.info("Building pipeline ...")

    p.run()

if __name__ == '__main__':
    run()
import apache_beam as beam
from apache_beam.transforms.sql import SqlTransform
from apache_beam.options.pipeline_options import PipelineOptions

def run():
    input_path = 'gs://stock-market-dataset/preprocessed/'
    output_table = 'your-project:dataset.aggregated_data'

    query = """
        SELECT EXTRACT(YEAR FROM TIMESTAMP(Date)) AS Year,
               EXTRACT(QUARTER FROM TIMESTAMP(Date)) AS Quarter,
               COUNT(Date) AS Market_Open_Days
        FROM PCOLLECTION
        GROUP BY Year, Quarter
        ORDER BY Year, Quarter
    """

    options = PipelineOptions()
    p = beam.Pipeline(options=options)

    (
        p
        | 'ReadPreprocessedData' >> beam.io.ReadFromText(input_path)
        | 'SQLQuery' >> SqlTransform(query)
        | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
            output_table,
            schema='Year:INTEGER, Quarter:INTEGER, Market_Open_Days:INTEGER',
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
        )
    )

    p.run().wait_until_finish()

if __name__ == '__main__':
    run()

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

class ConvertToDict(beam.DoFn):
    def process(self, element):
        try:
            Date, Open, High, Low, Close, Volume, OpenInt = element.split(',')
            yield {
                "Date": Date.strip(),
                "Open": float(Open.strip()),
                "High": float(High.strip()),
                "Low": float(Low.strip()),
                "Close": float(Close.strip()),
                "Volume": int(Volume.strip()),
                "OpenInt": int(OpenInt.strip())
            }
        except ValueError:
            yield beam.pvalue.TaggedOutput('error', element)

def run():
    input_path = 'gs://stock-market-dataset/archive'
    output_path = 'gs://stock-market-dataset/preprocessed/'

    options = PipelineOptions()
    p = beam.Pipeline(options=options)

    result = (
        p
        | 'ReadFromGCS' >> beam.io.ReadFromText(input_path, skip_header_lines=1)
        | 'ConvertToDict' >> beam.ParDo(ConvertToDict()).with_outputs('error', main='valid')
    )

    result.valid | 'WriteValidRecords' >> beam.io.WriteToText(output_path)
    result.error | 'WriteInvalidRecords' >> beam.io.WriteToText(output_path + 'errors')

    p.run().wait_until_finish()

if __name__ == '__main__':
    run()

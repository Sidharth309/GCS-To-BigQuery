I have demonstrated a few concepts which I feel would be necessary while building and implementing an ETL pipeline such as using 
ZetaSQL for data analysis in the pipeline itself and a good code practice of dead letter queue implementation.
The below code analyzes a huge 1.5 GB stock market dataset stored in GCS and writes it to BigQuery using SQL in Dataflow if
the expected input is given.

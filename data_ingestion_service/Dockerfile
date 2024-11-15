FROM apache/beam_python3.8_sdk:latest
COPY data_ingestion_service.py /app/
WORKDIR /app
ENTRYPOINT ["python", "data_ingestion_service.py"]

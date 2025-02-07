FROM python:3.9.1
RUN apt-get install wget
RUN apt-get update && apt-get install -y \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*
RUN pip install pandas sqlalchemy psycopg2 requests pyarrow argparse os

WORKDIR /app
COPY ingest_data.py ingest_data.py

 ENTRYPOINT [ "python", "ingest_data.py" ]

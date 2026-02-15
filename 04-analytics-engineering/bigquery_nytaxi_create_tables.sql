CREATE OR REPLACE EXTERNAL TABLE data-engineer-camp-2026.nytaxi.green_tripdata_all
OPTIONS (
  format = 'CSV',
  uris = ['gs://data-lake-bucket-2026/green_tripdata_*.csv']
);

CREATE OR REPLACE TABLE data-engineer-camp-2026.nytaxi.green_tripdata
AS
SELECT * FROM `data-engineer-camp-2026.nytaxi.green_tripdata_all`;


CREATE OR REPLACE EXTERNAL TABLE data-engineer-camp-2026.nytaxi.yellow_tripdata_all
OPTIONS (
  format = 'CSV',
  uris = ['gs://data-lake-bucket-2026/yellow_tripdata_*.csv']
);

CREATE OR REPLACE TABLE data-engineer-camp-2026.nytaxi.yellow_tripdata
AS
SELECT * FROM `data-engineer-camp-2026.nytaxi.yellow_tripdata_all`;
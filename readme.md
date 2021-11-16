# SONG PLAY ANALYSIS WAREHOUSE

## Propose of the project

This project has been developed to shift from data warehouse to data lake due to the tremendous growth of sparkify data

## About datasets

The songs data & log data are in JSON format that are stored in Amazon S3

## Tech stack

- Python
- Spark
- Amazon RDS
- Notion for project management
- Github for Version control

## Usage manual

- Create dwh.cfg file to store aws credentials
- Execute `etl.py` to transform the json file stored in aws to aws parquet, which will be stored in S3

## Files description

The project consists of a single file names etl.py, which is responsible for transformation of semi-structured json files. The project also include etl.ipynb which was used for testing purposes.

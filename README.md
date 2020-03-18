# S3 Data Lake using Spark (Amazon EMR) for Song Plays Analysis

## Objectives
The aim of this project it to establish a data lake in Amazon S3 to enable analysis of user-level data for a music streaming app. A ELT data pipeline is created which uses a Spark cluster on Amazon EMR to extract raw files from S3 buckets, transform them, if need be, and store them in the data lake. The output is saved as `.parquet` files to allow performant use of schema-on-read.

## Relevant Files
Project has the following main files:

| File   | Description |
---------|-------------
`dl.cfg` | AWS access key configuration
`elt.py` | script for extracting data residing in JSON files and populating the data lake

## Data Pipeline Specifics
The following are a few specifics of the ELT stages:

1. Datasets are contained in `.json` files. They are read into the Spark cluster with schema inference turned on.
2. Datasets are transformed to data frames following a STAR schema. Given the small number of dimensions song play logs need to be analysed across and the size of the dataset, redundancy could be easily tolerated. Hence, easier joins were favored to enable speed of data analysis.
3. Transformations included:
    - Selecting the appropriate columns and naming them according to the given schema.
    - Dropping duplicate rows.
    - Creating columns using user-defined functions, e.g. for timestamp parsing and datetime creation.
    - Joining songs metadata and song play logs datasets to fetch `song_id` and `artist_id` wherever possible.
4. The transformed data frames are saved in `.parquet` format, partitioned appropriately to speed up common queries. For example:
    - `songplays` and `time` data frames were partitioned using year and month to allow faster temporal analysis of the dataset.
    - `songs` data frame was partitioned using year and artist to aid temporal analysis by artists of song plays.

## Using the Files
The files should be on Spark cluster's master node. `dl.cfg` should contain AWS credentials. Execute `elt.py` to populate the date lake.  Use `elt.py -h` for help on using the file.

## Example Queries on Data Lake
```python
df = spark.read.parquet("s3://ud-sparkify-data-lake/analytics/songplays.parquet")

# query: total number of song plays
# expected result: 6820
print(f"Total song plays: { df.count() }")

# query: number of song plays where metadata is available with us
# expected result: 319
print(f"Song plays with known song_id: { df.filter(col("song_id").isNotNull()).count() }")
```

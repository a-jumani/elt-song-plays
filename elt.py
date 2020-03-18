import argparse
import configparser
import os
from pyspark.sql import SparkSession
import textwrap

config = configparser.ConfigParser()
config.read('dl.cfg')

# set up environment variables to enable S3 access
os.environ['AWS_ACCESS_KEY_ID'] = config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS_SECRET_ACCESS_KEY']

OUTPUT_FOLDER = "analytics"


def create_spark_session():
    """
    Create a spark session or get it if it already exists.

    Args:
        None
    Returns:
        session object
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Process songs dataset to create songs and artists table. The tables
    are stored as appropriately partitioned parquet files under output_data.

    Args:
        spark session object
        input_data s3 bucket path for input data
        output_data s3 bucket path to store output data
    Returns:
        None
    """
    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data", '*', '*', '*', "*.json")

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(
        "song_id", "title", "artist_id", "year", "duration")

    # write songs table to parquet files partitioned by year and artist
    songs_table.write \
               .partitionBy("year", "artist_id") \
               .parquet(
                   os.path.join(output_data, OUTPUT_FOLDER, "songs.parquet"))

    # extract columns to create artists table
    artists_table = df.selectExpr("artist_id",
                                  "artist_name as name",
                                  "artist_location as location",
                                  "artist_latitude as latitude",
                                  "artist_longitude as longitude") \
                      .dropDuplicates()

    # write artists table to parquet files
    artists_table.write.partitionBy("artist_id").parquet(
        os.path.join(output_data, OUTPUT_FOLDER, "artists.parquet"))


def process_log_data(spark, input_data, output_data):
    """
    Process songplays logs to create user, time and songplay tables. The tables
    are stored as appropriately partitioned parquet files under output_data.

    Args:
        spark session object
        input_data s3 bucket path for input data
        output_data s3 bucket path to store output data
    Returns:
        None
    """
    pass


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter,
        description=textwrap.dedent("\
            Extract and load song plays data in S3.")
    )

    # argument for input data
    parser.add_argument(
        "--input",
        required=True,
        metavar="S3_PATH",
        help="s3[a]://path/to/input/data/"
    )

    # argument for output path
    parser.add_argument(
        "--output",
        required=True,
        metavar="S3_PATH",
        help="s3[a]://path/to/output/dir/"
    )

    # parse arguments
    args = parser.parse_args()
    input_data = args.input
    output_data = args.output

    # create spark session
    spark = create_spark_session()

    # process datasets
    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)

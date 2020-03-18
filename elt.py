import argparse
import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, \
    date_format
from pyspark.sql.types import *
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
    # get filepath to log data file
    log_data = os.path.join(input_data, "log_data", '*', '*', "*.json")

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table
    users_table = df.selectExpr("userId as user_id",
                                "firstName as first_name",
                                "lastName as last_name",
                                "gender",
                                "level") \
                    .dropDuplicates(["user_id"])

    # write users table to parquet files
    users_table.write.partitionBy("user_id").parquet(
        os.join.path(output_data, OUTPUT_FOLDER, "users.parquet"))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: int(x) / 1000.0, DoubleType())
    df = df.withColumn("timestamp", get_timestamp(df.ts))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda ts: datetime.fromtimestamp(ts), TimestampType())
    df = df.withColumn("datetime", get_datetime(df.timestamp))

    # extract columns to create time table
    time_table = df.selectExpr("datetime as start_time") \
                   .dropDuplicates() \
                   .withColumn("hour", hour(col("start_time"))) \
                   .withColumn("day", dayofmonth(col("start_time"))) \
                   .withColumn("week", weekofyear(col("start_time"))) \
                   .withColumn("month", month(col("start_time"))) \
                   .withColumn("year", year(col("start_time"))) \
                   .withColumn("weekday", date_format(col("start_time"), "u"))

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(
        os.join.path(output_data, OUTPUT_FOLDER, "time.parquet"))

    # read in song data to use for songplays table
    song_df = spark.read.json(
        os.path.join(input_data, "song_data", '*', '*', '*', "*.json"))

    # extract columns from joined song and log datasets to create songplays
    # table
    songplays_table = df.alias('a') \
                        .join(song_df.alias('b'),
                              (col("a.song") == col("b.title")) &
                              (col("a.artist") == col("b.artist_name")) &
                              (col("a.length") == col("b.duration")),
                              "left") \
                        .selectExpr("a.datetime as start_time",
                                    "a.userId as user_id",
                                    "a.level as level",
                                    "b.song_id as song_id",
                                    "b.artist_id as artist_id",
                                    "a.sessionId as session_id",
                                    "a.location as location",
                                    "a.userAgent as user_agent") \
                        .withColumn("year", year(col("start_time"))) \
                        .withColumn("month", month(col("start_time")))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(
        os.join.path(output_data, OUTPUT_FOLDER, "songplays.parquet"))


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

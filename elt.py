import argparse
import textwrap


def create_spark_session():
    """
    Create a spark session or get it if it already exists.

    Args:
        None
    Returns:
        session object
    """
    pass


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
    pass


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

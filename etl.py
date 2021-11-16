import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read("dl.cfg")

os.environ["AWS_ACCESS_KEY_ID"] = config["AWS"]["AWS_ACCESS_KEY_ID"]
os.environ["AWS_SECRET_ACCESS_KEY"] = config["AWS"]["AWS_SECRET_ACCESS_KEY"]


def create_spark_session():
    """Returns spark session object

    Returns:
        [Object]: pyspark.sql.session.SparkSession object
    """
    spark = SparkSession.builder.config(
        "spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0"
    ).getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """This method :
        - reads all song_data json file from the s3
        - transforms the data to dimensional table
        - store the tables into s3 using columnar database parquet


    Args:
        spark (object): pyspark.sql.session.SparkSession object
        input_data (string): path of s3 for input json file
        output_data (string): s3 path to write parquet tables
    """

    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data/A/A/A/")

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(
        ["song_id", "title", "artist_id", "year", "duration"]
    ).dropDuplicates()

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").parquet(
        f"{output_data}songs", partitionBy=["year", "artist_id"]
    )

    # extract columns to create artists table
    artists_table = df.select(
        [
            "artist_id",
            "artist_name",
            "artist_location",
            "artist_latitude",
            "artist_longitude",
        ]
    ).dropDuplicates()

    artists_table = artists_table.toDF(
        "artist_id", "name", "location", "latitude", "longitude"
    )

    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(os.path.join(output_data, "artists"))


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "./sparkify_bigdata/"

    process_song_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

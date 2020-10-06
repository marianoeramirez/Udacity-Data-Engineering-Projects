import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType, DoubleType, StructType, StructField, LongType, TimestampType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config["default"]['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config["default"]['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    This function we get or create the spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.3") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Process song file that use spark to extract, load and transform the data, then is saved to the
    song and artist information
    :param spark: it is the session to the spark cluster
    :param input_data: this is the path to the directory of songs data
    :param output_data: this is the path to the output directory to save the song and artist data
    :returns: None
    """

    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"

    song_schema = StructType([
        StructField('artist_id', StringType(), True),
        StructField('artist_latitude', DoubleType(), True),
        StructField('artist_location', StringType(), True),
        StructField('artist_longitude', DoubleType(), True),
        StructField('duration', DoubleType(), True),
        StructField('num_songs', LongType(), True),
        StructField('artist_name', StringType(), True),
        StructField('song_id', StringType(), True),
        StructField('title', StringType(), True),
        StructField('year', LongType(), True),
    ])
    # read song data file
    df = spark.read.schema(song_schema).json(song_data, mode='PERMISSIVE', columnNameOfCorruptRecord='corrupt_record')

    # extract columns to create songs table
    songs_table = df.selectExpr("song_id", "title", "artist_id", "year", "duration").drop_duplicates(subset=['song_id'])

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy(['year', 'artist_id']).parquet(f"{output_data}songs.parquet", mode="overwrite")

    # extract columns to create artists table
    artists_table = df.selectExpr("artist_id", "artist_name as name", "artist_location as location",
                                  "artist_latitude as latitude", "artist_longitude as longitude").drop_duplicates(
        subset=['artist_id'])

    # write artists table to parquet files
    artists_table.write.parquet(f"{output_data}artists.parquet", mode="overwrite")


def process_log_data(spark, input_data, output_data):
    """
    Process log file that use spark to extract, load and transform the data, then is saved to the
    song plays, time and users information
    :param spark: it is the session to the spark cluster
    :param input_data: this is the path to the directory of log data
    :param output_data: this is the path to the output directory to save the  song plays, time and users information
    :returns: None
    """

    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    event_schema = StructType([
        StructField('auth', StringType(), True),
        StructField('firstName', StringType(), True),
        StructField('gender', StringType(), True),
        StructField('itemInSession', LongType(), True),
        StructField('lastName', StringType(), True),
        StructField('length', DoubleType(), True),
        StructField('level', StringType(), True),
        StructField('location', StringType(), True),
        StructField('method', StringType(), True),
        StructField('page', StringType(), True),
        StructField('registration', DoubleType(), True),
        StructField('sessionId', LongType(), True),
        StructField('song', StringType(), True),
        StructField('status', LongType(), True),
        StructField('ts', LongType(), True),
        StructField('userAgent', StringType(), True),
        StructField('userId', StringType(), True),
    ])

    # read log data file
    df = spark.read.schema(event_schema).json(log_data, mode='PERMISSIVE', columnNameOfCorruptRecord='corrupt_record')

    # filter by actions for song plays
    df = df.where(df.page == "NextSong")

    # extract columns for users table
    users_table = df.select("userId", "firstName", "lastName", "gender", "level").drop_duplicates(subset=['userId'])

    # write users table to parquet files
    users_table.write.parquet(f"{output_data}users.parquet", mode="overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000), TimestampType())
    df = df.withColumn("start_time", get_timestamp(df.ts))

    # extract columns to create time table
    time_table = df.select(F.col("start_time"), F.year("start_time").alias("year"),
                           F.hour("start_time").alias("hour")
                           , F.dayofmonth("start_time").alias("day"),
                           F.month("start_time").alias("month"),
                           F.weekofyear("start_time").alias("weekofyear"),
                           F.dayofweek("start_time").alias("weekday")).drop_duplicates(subset=['start_time'])

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy(['year', 'month']).parquet(f"{output_data}time.parquet", mode="overwrite")

    # read in song data to use for songplays table
    song_df = spark.read.parquet(f"{output_data}songs.parquet").drop_duplicates()

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = df.join(song_df, df.song == song_df.song_id, "left") \
        .select("start_time", F.year("start_time").alias("year"), F.month("start_time").alias("month"),
                "userId", "song_id", "level", "sessionId", "location", "userAgent")

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy(['year', 'month']).parquet(f"{output_data}songplays.parquet", mode="overwrite")


def main():
    """
    Main function of the applicattion
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://data-lake-udacity-mariano/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

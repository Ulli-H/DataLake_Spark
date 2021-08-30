import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    """
    Creates an new spark session or gets existing session.
    :return: SparkSession
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Processes song_data (JSON) from s3 input bucket and stores it in s3 output bucket as parquet files.
    :param spark: SparkSession
    :param input_data: s3 location of all song_data
    :param output_data: s3 location of output data

    """

    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')

    song_schema = StructType([
        StructField("artist_id", StringType()),
        StructField("artist_latitude", DoubleType()),
        StructField("artist_longitude", StringType()),
        StructField("artist_location", StringType()),
        StructField("artist_name", StringType()),
        StructField("duration", DoubleType()),
        StructField("num_songs", IntegerType()),
        StructField("title", StringType()),
        StructField("year", IntegerType())
    ])

    # read song data file
    df = spark.read.load(song_data, schema=song_schema, format='json')

    # extract columns to create songs table
    songs_table = (df.select(["artist_id", "title", "year", "duration"])
                   .dropDuplicates().withColumn("song_id", monotonically_increasing_id()))
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy('year', 'artist_id')\
        .parquet(output_data + 'songs/songs_table.parquet')

    # extract columns to create artists table
    artists_table = df.select(["artist_id",  "artist_name", "artist_location", "artist_latitude", "artist_longitude"])
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + 'artist/artist_table.parquet').dropDuplicates()


def process_log_data(spark, input_data, output_data):
    """
    Processes log_data (JSON) from s3 input bucket and stores it in s3 output bucket as parquet files.
    :param spark: SparkSession
    :param input_data: s3 location of all log_data
    :param output_data: s3 location of output data
    """
    # get filepath to log data file
    log_data = os.path.join(input_data, "log_data/*/*/*.json")

    # read log data file
    df = spark.read.load(log_data, format='json')
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.select(["userId", "firstName", "lastName", "gender", "level"]).dropDuplicates()

    # write users table to parquet files
    users_table.write.parquet(output_data + 'users/users_table.parquet')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x:x/1000, TimestamType())
    df = df.withColumn("timestamp", get_timestamp(df.ts) )
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x), TimestampType())
    df = df.withColumn("datetime", get_datetime(df.timestamp))
    
    # extract columns to create time table
    df = df.withColumn("hour", hour("datetime")) \
        .withColumn("day", dayofmonth("datetime")) \
        .withColumn("week", weekofyear("datetime")) \
        .withColumn("month", month("datetime")) \
        .withColumn("year", year("datetime")) \
        .withColumn("weekday", dayofweek("datetime"))

    time_table = df.select("datetime", "hour", "day", "week", "month", "year", "weekday")
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month')\
        .parquet(output_data + 'time/time_table.parquet')

    # read in song data to use for songplays table
    song_df = spark.read.parquet(os.path.join(output_data, "songs/*/*/*"))
    songs_logs = df.join(song_df, (df.song == song_df.title))

    # extract columns from joined song and log datasets to create songplays table 
    artist_df = spark.read.parquet(os.path.join(output_data, "artists"))
    artists_songs_log = songslog.join(artist_df, (songs_logs.artist == artist_df.name))

    songplays = artists_songs_log.join(time_table, artists_songs_log.ts == time_table.ts, 'left')\
        .drop(artists_songs_log.year)

    # write songplays table to parquet files partitioned by year and month
    songplays_table = songplays.select(
        col("datetime"),
        col("userId"),
        col("level"),
        col("song_id"),
        col("artist_id"),
        col("sessionId"),
        col("artist_location"),
        col("userAgent"),
        col("year"),
        col("month"),
    ).repartition("year", "month")

    songplays_table.write.partitionBy('year', 'month')\
        .parquet(output_data + 'songplays/songplays_table.parquet')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify-data-udend/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

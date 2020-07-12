import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.types import TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = "data/song_data/A/B/C/*.json"
    
    # read song data file
    df = spark.read.json(song_data)
    
    # create view for song table to write sql query
    df.createOrReplaceTempView("song_data_table")

    # extract columns to create songs table
    songs_table = spark.sql('''
                            SELECT DISTINCT sd.song_id,
                                   sd.title,
                                   sd.artist_id,
                                   sd.year,
                                   sd.duration
                            FROM song_data_table sd
                            WHERE sd.song_id IS NOT NULL
    ''')
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write \
               .partitionBy("year", "artist_id") \
               .parquet('songs/')

    # extract columns to create artists table
    artists_table = spark.sql('''
                              SELECT DISTINCT sd.artist_id,
                                     sd.artist_name      AS name,
                                     sd.artist_location  AS location,
                                     sd.artist_latitude  AS latitude,
                                     sd.artist_longitude AS longitude
                              FROM song_data_table sd
                              WHERE sd.artist_id IS NOT NULL
    ''')
    
    # write artists table to parquet files
    artists_table.write \
                 .parquet('artists/')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = "data/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')
    
    # craete view for log data to write queries
    df.createOrReplaceTempView("log_data_table")

    # extract columns for users table    
    users_table = spark.sql('''
                              SELECT DISTINCT ld.userId as user_id, 
                                     ld.firstName as first_name,
                                     ld.lastName as last_name,
                                     ld.gender as gender,
                                     ld.level as level
                              FROM log_data_table ld
                              WHERE ld.userId IS NOT NULL
    ''')
    
    # write users table to parquet files
    users_table.write \
               .parquet('users/')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x : datetime.utcfromtimestamp(int(x)/1000), TimestampType())
    df = df.withColumn("start_time", get_timestamp("ts"))
    
    
    # extract columns to create time table
    time_table = df.withColumn("hour",hour("start_time"))\
                    .withColumn("day",dayofmonth("start_time"))\
                    .withColumn("week",weekofyear("start_time"))\
                    .withColumn("month",month("start_time"))\
                    .withColumn("year",year("start_time"))\
                    .withColumn("weekday",dayofweek("start_time"))\
                    .select("ts","start_time","hour", "day", "week", "month", "year", "weekday").drop_duplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write \
              .partitionBy("year", "month") \
              .parquet("time/")

    # read in song data to use for songplays table
    song_df = spark.read.parquet("songs/")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = (
        df.withColumn("songplay_id", monotonically_increasing_id())
          .join(song_df, song_df.title == df.song)
          .select(
            "songplay_id",
            col("start_time"),
            col("userId").alias("user_id"),
            "level",
            "song_id",
            "artist_id",
            col("sessionId").alias("session_id"),
            "location",
            col("userAgent").alias("user_agent")
          )
    )
    
    songplays_table = songplays_table.join(time_table, songplays_table.start_time == time_table.start_time, how="inner")\
                        .select(
                                "songplay_id", 
                                songplays_table.start_time, 
                                "user_id", "level", "song_id", "artist_id", "session_id", "location", "user_agent", "year", "month")


    # write songplays table to parquet files partitioned by year and month
    songplays_table.write \
                   .partitionBy("year", "month") \
                   .parquet("songplays/")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

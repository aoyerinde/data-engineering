import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    This function initiates a Spark Session with which we will process out data
    with 
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    The function read song files from s3, processing is done with Spark and tables are saved back to an output folder in an S3 bucket
    ::param spark: initialized spark session 
    ::param input_data: source dataset directory
    ::param output_data: destination file directory
    """
    # get filepath to song data file
    song_data = "data/song_data_files/*/*/*/*"
    
    # read song data file
    song_data = spark.read.json(song_data)
    song_data.createOrReplaceTempView("songs")

    # extract columns to create songs table
    songs_table = spark.sql("SELECT distinct song_id, title as song_title, artist_id, year, duration FROM songs")
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data+"song.parquet", mode = "overwrite")

    # extract columns to create artists table
    artists_table = spark.sql("SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude FROM songs")
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data+"artist.parquet", mode = "overwrite")


def process_log_data(spark, input_data, output_data):
    """
    The function read log files from s3, processing is done with Spark and tables are saved back to an output folder in an S3 bucket
    ::param spark: initialized spark session 
    ::param input_data: source dataset directory
    ::param output_data: destination file directory
    """
    # get filepath to log data file
    log_data = "data/log_data_files"

    # read log data file
    df = spark.read.json(log_data)
    df.createOrReplaceTempView("staging_events")
    
    # filter by actions for song plays
    df = spark.sql("SELECT *, cast(ts/1000 as Timestamp) as timestamp from staging_events where page = 'NextSong'")
    
    user_query = ("""
    select staging_a.userId, staging_a.firstName, staging_a.lastName, staging_a.gender, staging_a.level
    from staging_events staging_a inner join (
    select userId, max(ts) as ts 
    from staging_events 
    group by userId, page
    ) staging_b on staging_a.userId = staging_b.userId and staging_a.ts = staging_b.ts
    """)

    # extract columns for users table    
    user_table = spark.sql(user_query).dropDuplicates(['userId', 'level'])
    
    # write users table to parquet files
    user_table.write.parquet(path = output_data + "users.parquet")

    # create timestamp column from original timestamp column
    #get_timestamp = udf()
    #df =
    time_query = """select distinct timestamp as start_time, 
    hour(timestamp) as hour, 
    day(timestamp) as day, 
    weekofyear(timestamp) as week, 
    month(timestamp) as month, 
    year(timestamp) as year, 
    weekday(timestamp) as weekday
    from staging_events"""
    
    # extract columns to create time table
    time_table =  spark.sql(time_query)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(path = output_data + "time.parquet")

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "songs.parquet")
    song_df.createOrReplaceTempView("song_play")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_query = """select a.timestamp as start_time, a.userId, a.level, b.song_id, b.artist_id, a.sessionId, a.location,
    a.userAgent, year(a.timestamp) as year, month(a.timestamp) as month 
    from song_play as a 
    inner join songs as b on a.song = b.song_title"""
    songplays_table = spark.sql(songplays_query)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(path = output_data + "songplays.parquet")


def main():
    """
    This function calls indivdual functions to create spark, process
    song_data and process_log_data and runs them
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://data-lake-project-oyerinde/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

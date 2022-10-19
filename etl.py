import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as t
from pyspark.conf import SparkConf ## adding this myself - may not need it

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

conf = SparkConf().set("spark.jars", "/org.apache.hadoop:hadoop-aws:2.7.0.jar") # just trying this per https://knowledge.udacity.com/questions/886926

def create_spark_session():
    # updating per: https://knowledge.udacity.com/questions/886926 - I downloaded hadoop-aws-2.7.0.jar and placed in the workspace
    # older config line : # .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
#     spark = SparkSession \
#         .builder \
#         .config("spark.jars", "org.apache.hadoop:hadoop-aws:2.7.0") \
#         .getOrCreate()
    spark = SparkSession \
        .builder \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    # unzipped from terminal because spark cannot read zipped files - https://knowledge.udacity.com/questions/145486
    song_data = f'{input_data}song_data/*/*/*/*.json' 
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(
            'song_id', 'title', 'artist_id', 'year', 'duration'
    ).dropDuplicates() # in case there are repeat songs
    
    # write songs table to parquet files partitioned by year and artist
    # using artist_id instead of artist in case there are two artists with the same name
    songs_table.write.partitionBy('year', 'artist_id').parquet(f'{output_data}songs_table')

#     # extract columns to create artists table
    artists_table = df.select(
        'artist_id', 
        F.col('artist_name').alias('name'), 
        F.col('artist_location').alias('location'), 
        F.col('artist_latitude').alias('latitude'), 
        F.col('artist_longitude').alias('longitude')
    ).dropDuplicates() # in case there are repeat artists
    
#     # write artists table to parquet files
    artists_table.write.parquet(f'{output_data}artists_table')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = f'{input_data}log_data/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where(F.col('page') == 'NextSong').cache()

    # extract columns for users table    
    users_table = df.select(
        F.col('userId').alias('user_id'), 
        F.col('firstName').alias('first_name'), 
        F.col('lastName').alias('last_name'), 
        'gender', 
        'level'
    )
    
    # write users table to parquet files
    users_table.write.parquet(f'{output_data}users_table')

    # create timestamp column from original timestamp column
    # just changing to seconds. I don't see what else would be useful here
    df = df.withColumn('timestamp', (F.col('ts')/1000)) ## converting to s from ms, as .from_unixtime() works with seconds
    
    # create datetime column from original timestamp column  ## taking this to mean get date from seconds
    df = df.withColumn('datetime', F.from_unixtime(F.col('timestamp')))
    
    # extract columns to create time table
    time_table = df.select(F.col('datetime').alias('start_time')) \
                        .withColumn('hour', F.hour(F.col('start_time'))) \
                        .withColumn('day', F.dayofmonth(F.col('start_time'))) \
                        .withColumn('week', F.weekofyear(F.col('start_time'))) \
                        .withColumn('month', F.month(F.col('start_time'))) \
                        .withColumn('year', F.year(F.col('start_time'))) \
                        .withColumn('weekday', F.dayofweek(F.col('start_time'))) 
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(output_data + 'time_table')

    # read in song data to use for songplays table
    song_df = spark.read.json(f'{input_data}song_data/*/*/*/*.json')

    # extract columns from joined song and log datasets to create songplays table 
    # need to match records based on song AND artist
    songplays_table = df.join(song_df,
                              on = (df.artist == song_df.artist_name) & (df.song == song_df.title), # song filter alternative: & (df.length == song_df.duration) 
                              how = 'inner'
                             ).withColumn('songplay_id', F.monotonically_increasing_id()) \
    .select(
        'songplay_id',
        df.datetime.alias('start_time'), # not sure if I should be using a different time here, but opting for consistency with time_table for now
        df.userId.alias('user_id'),
        df.level,
        song_df.song_id,
        song_df.artist_id,
        df.sessionId.alias('session_id'),
        df.location,
        df.userAgent.alias('user_agent'),
    ).withColumn('month', F.month(F.col('start_time'))) \
                        .withColumn('year', F.year(F.col('start_time'))) \
    
    # returns one non-null record on small dataset which is correct based on https://knowledge.udacity.com/questions/215393

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').parquet(f'{output_data}songplays_table')

def main():
    spark = create_spark_session()
    input_data = "/home/workspace/data/" # "s3a://udacity-dend/" # "s3a://udacity-dend/song-data/"
    output_data = "/home/workspace/data/" # "s3a://udacity-dend/"
    
    process_song_data(spark, input_data, output_data)   
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
    
    
#     df_temp = spark.read.json('/home/workspace/data/log_data/*.json')
#     df_temp = df_temp.withColumn('timestamp', (F.col('ts')/1000)) ## converting to s from ms, as from_unixtime() works with seconds
#     df_temp = df_temp.withColumn('datetime', F.from_unixtime(F.col('timestamp')))
#     df_temp = df_temp.withColumn('hour', F.hour(F.col('datetime'))) \
#                         .withColumn('day', F.dayofmonth(F.col('datetime'))) \
#                         .withColumn('week', F.weekofyear(F.col('datetime'))) \
#                         .withColumn('month', F.month(F.col('datetime'))) \
#                         .withColumn('year', F.year(F.col('datetime'))) \
#                         .withColumn('weekday', F.dayofweek(F.col('datetime'))) 
#     df_temp.printSchema()
#     df_temp.select('ts', 'timestamp', 'datetime', 'hour', 'day', 'week', 'month', 'year').show(truncate=False) 

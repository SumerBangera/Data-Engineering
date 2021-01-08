import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
        Create or retrieve a Spark Session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Description: Loads song_data from S3 bucket, processes it by extracting
                 the songs and artist tables and loads it back to S3
    Parameters:
        spark: cursor object (SparkSession)
        input_path: path to the S3 bucket containing song_data
        output_path: path to S3 bucket where the dimensional tables 
                     will be stored in parquet format
    Returns:
        None
    """ 
    
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"        # "song_data/A/A/A/*.json" for sample data
    # song_data = input_data + "song-data-unzipped/song_data/*/*/*/*.json"    for using data locally
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration").dropDuplicates()
    
    # using spark sql instead of dataframes
#     df.createOrReplace("song_df")
#     songs_table = spark.sql("""
#                                SELECT song_id, title, artist_id, year, duration
#                                FROM song_df
#                                WHERE song_id IS NOT NULL
#                             """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data + 'songs_table',
                              mode = 'overwrite',
                              partitionBy = ['year', 'artist_id'])
    

    # extract columns to create artists table
    artists_table = df.select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude").dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + 'artists_table',
                                mode = 'overwrite')


def process_log_data(spark, input_data, output_data):
    """
    Description: Loads log_data from S3 bucket, processes it by extracting
                 the songplays fact table along with user, time and song dimension tables, 
                 and then loads it back to S3
    Parameters:
        spark: cursor object (SparkSession)
        input_path: path to the S3 bucket containing log_data
        output_path: path to S3 bucket where the dimensional tables 
                     will be stored in parquet format
    Returns:
        None
    """ 
    
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"
#     log_data = input_data + "log-data-unzipped/*.json"      for using data locally

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page=="NextSong")

    # extract columns for users table    
    users_table = df.select('userId', 'firstName', 'lastName','gender', 'level').dropDuplicates() 
    
    # write users table to parquet files
    users_table.write.parquet(output_data + 'users_table',
                              mode = 'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn('start_time', get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_date = udf(lambda x: datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d'))
    df = df.withColumn('date', get_date(df.ts))
    
    # extract columns to create time table
    time_table = df.select('start_time').withColumn('year', year(col('start_time'))) \
                                        .withColumn('month', month(col('start_time'))) \
                                        .withColumn('week', weekofyear(col('start_time'))) \
                                        .withColumn('weekday', date_format(col('start_time'),'E')) \
                                        .withColumn('day', dayofmonth(col('start_time'))) \
                                        .withColumn('hour', hour(col('start_time'))) \
                                    .dropDuplicates() 

    # dayofweek vs date_format ref: https://stackoverflow.com/questions/25006607/how-to-get-day-of-week-in-sparksql
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data + 'time_table',
                             mode = 'overwrite',
                             partitionBy = ['year','month'])

    # read in song data to use for songplays table
    song_data = input_data + "song_data/*/*/*/*.json"      # "song_data/A/A/A/*.json" for sample data
#     song_data = input_data + "song-data-unzipped/song_data/*/*/*/*.json"     for using data locally
    
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table 
    song_df.createOrReplaceTempView('song_df')
    df.createOrReplaceTempView('log_df')
    time_table.createOrReplaceTempView('time_table')
    
    songplays_table = spark.sql("""SELECT DISTINCT  
                                             t.start_time,
                                             t.year as year,
                                             t.month as month,                                             
                                             l.userId, 
                                             l.level, 
                                             s.song_id,
                                             s.artist_id, 
                                             l.sessionid, 
                                             s.artist_location,
                                             l.useragent
                                FROM song_df s
                                JOIN log_df l
                                     ON s.artist_name = l.artist
                                     AND s.title = l.song
                                     AND s.duration = l.length
                                JOIN time_table t
                                     ON t.start_time = l.start_time
                         """).dropDuplicates()
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data + 'songplays_table',
                                  mode = 'overwrite',
                                  partitionBy = ['year', 'month'])


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify-bucket2/"

#     input_data = "data/"                   # for using data locally
#     output_data = "data/parquet-files/"    # for using data locally

    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

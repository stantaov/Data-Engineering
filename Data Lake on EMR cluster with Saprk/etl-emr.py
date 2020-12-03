# load all libraries
import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import *
from pyspark.sql.functions import monotonically_increasing_id

os.environ['AWS_ACCESS_KEY_ID']='ADD YOUR ACCESS KEY'
os.environ['AWS_SECRET_ACCESS_KEY']='ADD YOUR SECRET ACCESS KEY'

def create_spark_session():
    '''
    This function creates a spark session
    '''
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()

    # this code speeds up parquet write
    sc = spark.sparkContext
    sc._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.algorithm.version", "2")
    sc._jsc.hadoopConfiguration().set("spark.speculation","false")

    return spark

def process_song_data(spark, input_data, output_data):
    '''
    This function reads data from S3 and transforms it via Spark processes and  writes data back to S3.
    
    Parameters:
    - spark: The spark session
    - input_data: The S3 path location not including the final folder
    - output_data: The output HDFS path to temporarily save the tables
    '''
    # create schema and read song data file 
    schema =  StructType([
                        StructField("num_songs", ShortType(), True),
                        StructField("artist_latitude", StringType(), True),
                        StructField("artist_location", StringType(), True),
                        StructField("artist_longitude", StringType(), True),
                        StructField("artist_name", StringType(), True),
                        StructField("song_id", StringType(), False),
                        StructField("title", StringType(), True),
                        StructField("artist_id", StringType(), False),
                        StructField("year", ShortType(), True),
                        StructField("duration", DoubleType(), True)
                    ])

    df_song_data = spark.read.schema(schema).json("s3a://udacity-dend/song_data/*/*/*/*.json")
    
    # write song data table to parquet files
    df_song_data.write.mode('overwrite').parquet(output_data + "/songs_data/")
    
    # copy parquet files from HDFS to S3
    os.system("s3-dist-cp --src hdfs:///dwh/songs_data --dest s3a://udacity-data-engineering-stan/data/songs_data")

    # extract columns to create songs table
    songs_table = df_song_data.select("song_id", "title", "artist_id", "year", "duration")
    
    # write songs table to parquet files partitioned by year and artist_id
    songs_table.write.mode('overwrite').partitionBy("year", "artist_id").parquet(output_data + "/songs_table/")
    
    # copy parquet files from HDFS to S3
    os.system("s3-dist-cp --src hdfs:///dwh/songs_table --dest s3a://udacity-data-engineering-stan/data/songs_table")

    # extract columns to create artists table              
    
    artists_table = df_song_data.select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude")
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + "/artists_table/")
    
    # copy parquet files from HDFS to S3
    os.system("s3-dist-cp --src hdfs:///dwh/artists_table --dest s3a://udacity-data-engineering-stan/data/artists_table")

def process_log_data(spark, input_data, output_data):
    '''
    This function reads data from S3 and transforms it via Spark processes and  writes data back to S3.
    
    Parameters:
    - spark: The spark session
    - input_data: The S3 path location not including the final folder
    - output_data: The output HDFS path to temporarily save the tables
    '''
    
    # create schema and read log data file
    schema =  StructType([
                            StructField("artist", StringType(), True),
                            StructField("auth", StringType(), True),
                            StructField("firstName", StringType(), True),
                            StructField("gender", StringType(), True),
                            StructField("itemInSession", ShortType(), True),
                            StructField("lastName", StringType(), True),
                            StructField("length", StringType(), True),
                            StructField("level", StringType(), True),
                            StructField("location", StringType(), True),
                            StructField("method", StringType(), True),
                            StructField("page", StringType(), True),
                            StructField("registration", StringType(), True),
                            StructField("sessionId", ShortType(), False),
                            StructField("song", StringType(), True),
                            StructField("status", StringType(), True),
                            StructField("ts", DoubleType(), True),
                            StructField("userAgent", StringType(), True),
                            StructField("userId", StringType(), False)
                        ])

    df_log_data = spark.read.schema(schema).json("s3a://udacity-dend/log_data/*/*/*.json")
    
    # filter by actions for song plays
    df_log_data = df_log_data.where(df_log_data.page == "NextSong")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp((int(x)/1000.0)), TimestampType())
    df_log_data = df_log_data.withColumn('timestamp', get_timestamp(df_log_data.ts))
    
    df_log_data = df_log_data.withColumn('start_time', date_format("timestamp", 'HH:mm:ss'))\
        .withColumn('hour', hour("timestamp"))\
        .withColumn('day', dayofmonth("timestamp"))\
        .withColumn('week', weekofyear("timestamp"))\
        .withColumn('month', month("timestamp"))\
        .withColumn('year', year("timestamp"))\
        .withColumn('weekday', date_format('timestamp', 'E'))
    
    # write log data table to parquet files
    df_log_data.write.mode('overwrite').parquet(output_data + "/log_data/")
    
    # copy parquet files from HDFS to S3
    os.system("s3-dist-cp --src hdfs:///dwh/log_data --dest s3a://udacity-data-engineering-stan/data/log_data")
    
    # extract columns for users table    
    users_table = df_log_data.select("start_time", "userId", "firstName", "lastName", "gender", "level").distinct()
    
    # create a new view table to use SQL
    users_table.createOrReplaceTempView("users_table")

    # select unique users with the most recent level status
    users_table = spark.sql("""
        SELECT userId, firstName, lastName, gender, level FROM (SELECT *, 
        ROW_NUMBER() OVER(PARTITION BY userId ORDER BY start_time DESC) AS num
        FROM users_table) AS tmp
        WHERE num = 1
    """)    
    
    # write users table to parquet files
    users_table.write.mode('overwrite').partitionBy("gender", "level").parquet(output_data + "/users_table/")
    
    # copy parquet files from HDFS to S3
    os.system("s3-dist-cp --src hdfs:///dwh/users_table --dest s3a://udacity-data-engineering-stan/data/users_table")
    
    # extract columns to create time table
    time_table = df_log_data.select("start_time", "hour", "day", "week", "month", "year", "weekday").distinct()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data + "/time_table/")
    
    # copy parquet files from HDFS to S3
    os.system("s3-dist-cp --src hdfs:///dwh/time_table --dest s3a://udacity-data-engineering-stan/data/time_table")

    # load song data to use for songplays table
    songs_df = spark.read.parquet(output_data + 'songs_data')
    
    # load artist data to get artist_id column for songplays table
    log_df = spark.read.parquet(output_data + 'log_data/*')
    
    # join df_join and df_song data frames and use it to create songplays_table
    df_join = songs_df.join(log_df, songs_df.title == log_df.song)

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df_join.select("start_time",
                                     "userId",
                                     "level",
                                     "song_id",
                                     "artist_id",
                                     "sessionId",
                                     "location",
                                     "userAgent").distinct()
    
    # create songplay_id column with unique id
    songplays_table = songplays_table.withColumn("songplay_id", monotonically_increasing_id())
    
    # rearrange songplays_table data frame
    songplays_table = songplays_table.select("songplay_id",
                                             "start_time",
                                             "userId",
                                             "level",
                                             "song_id",
                                             "artist_id",
                                             "sessionId",
                                             "location",
                                             "userAgent")

    # write songplays table to parquet files
    songplays_table.write.mode('overwrite').parquet(output_data + "/songplays_table/")
    
    # copy parquet files from HDFS to S3
    os.system("s3-dist-cp --src hdfs:///dwh/songplays_table --dest s3a://udacity-data-engineering-stan/data/songplays_table")

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "hdfs:///dwh/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

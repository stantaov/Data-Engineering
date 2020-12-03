# load all libraries
import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import *
from pyspark.sql.functions import monotonically_increasing_id



config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS CREDS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS CREDS']['AWS_SECRET_ACCESS_KEY']



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

    print(spark)
    return spark

def process_song_data(spark, input_data, output_data):
    '''
    This function reads data from S3 and transforms it via Spark processes and  writes data back to S3.
    
    Parameters:
    - spark: The spark session
    - input_data: The S3 path location not including the final folder
    - output_data: The output S3 bucket for the tables
    '''
    
    # get filepath to song data file
    song_data = "s3a://udacity-dend/song_data/*/*/*/*.json"
    
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

    df = spark.read.schema(schema).json(song_data)
    
    # write song data table to parquet files
    df.write.mode('overwrite').partitionBy("year", "artist_id").parquet(output_data + "/songs_data/")

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration").distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy("year", "artist_id").parquet(output_data + "/songs_table/")

    # extract columns to create artists table              
    artists_table = df.select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude")
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + "/artists_table/")


def process_log_data(spark, input_data, output_data):
    '''
    This function reads data from S3 and transforms it via Spark processes and writes data back to S3.
    
    Parameters:
    - spark: The spark session
    - input_data: The S3 path location not including the final folder
    - output_data: The output S3 bucket for the tables
    '''
    
    # get filepath to log data file
    log_data = "s3a://udacity-dend/log_data/*/*/*.json"

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

    df = spark.read.schema(schema).json(log_data)
    
    # filter by actions for song plays
    df = df.where(df.page == "NextSong")
    
    # write log data table to parquet files
    df.write.mode('overwrite').parquet(output_data + "/log_data/")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp((int(x)/1000.0)), TimestampType())
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    
    df = df.withColumn('start_time', date_format("timestamp", 'HH:mm:ss'))\
        .withColumn('hour', hour("timestamp"))\
        .withColumn('day', dayofmonth("timestamp"))\
        .withColumn('week', weekofyear("timestamp"))\
        .withColumn('month', month("timestamp"))\
        .withColumn('year', year("timestamp"))\
        .withColumn('weekday', date_format('timestamp', 'E'))
    
     # extract columns for users table    
    users_table = df.select("start_time", "userId", "firstName", "lastName", "gender", "level").distinct()
    
    # create a new view table to use SQL
    users_table.createOrReplaceTempView("users_table")
    
    # select unique users with the most recent level status
    users_table = spark.sql("""
        SELECT userId, firstName, lastName, gender, level FROM (SELECT *, 
        ROW_NUMBER() OVER(PARTITION BY userId ORDER BY start_time DESC) AS num
        FROM users_table) AS tmp
        WHERE num = 1
    """)
   
    # write users table to parquet files and partition by gender and level
    users_table.write.mode('overwrite').partitionBy("gender", "level").parquet(output_data + "/users_table/")    
    
    # extract columns to create time table
    time_table = df.select("start_time", "hour", "day", "week", "month", "year", "weekday").distinct()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data + "/time_table/")

    # load song data to use for songplays table
    songs_df = spark.read.parquet(output_data + 'songs_data')
    
    # load artist data to get artist_id column for songplays table
    log_df = spark.read.parquet(output_data + 'log_data/*')
    
    # join df_join and df_song data frames and use it to create songplays_table
    df_join = song_df.join(log_df, song_df.song == log_df.title)
    

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

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').parquet(output_data + "/songplays_table/")

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-data-engineering-stan/data/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

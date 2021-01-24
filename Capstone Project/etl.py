import pandas as pd
import warnings
warnings.filterwarnings("ignore")
import configparser
import os
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import datediff, to_date, date_format
from pyspark.sql.functions import year, month, dayofmonth
from pyspark.sql.functions import udf
from pyspark.sql.functions import col
from pyspark.sql.types import *


def create_spark_session():
    '''
    This function creates a spark session
    '''
    spark = SparkSession \
    .builder \
    .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.1.0-s_2.11,org.apache.hadoop:hadoop-aws:2.7.0") \
    .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.awsAccessKeyId", os.environ['AWS_ACCESS_KEY_ID']) \
    .config("spark.hadoop.fs.s3a.awsSecretAccessKey", os.environ['AWS_SECRET_ACCESS_KEY']) \
    .config("spark.driver.memory", "15g")\
    .enableHiveSupport() \
    .getOrCreate()
    # this code speeds up parquet write
    spark.conf.set("spark.sql.execution.arrow.enabled", "true")    
    sc = spark.sparkContext
    sc._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.algorithm.version", "2")
    sc._jsc.hadoopConfiguration().set("spark.speculation","false")
    print(spark)
    return spark

def i94_data(month, year):
    '''
    This function creates a parquet file from immigration data
    '''
    df_spark =spark.read.format('com.github.saurfang.sas.spark').load(f'../../data/18-83510-I94-Data-2016/i94_{month}{year}_sub.sas7bdat').drop_duplicates()
    df_spark = df_spark.where(col("dtadfile").isNotNull())
    df_spark = df_spark.withColumn('stayed_days', ( df_spark['depdate'] - df_spark['arrdate']))
    df_spark = df_spark.withColumn("allowed_stay_till",  to_date("dtaddto","MMddyyyy"))
    df_spark = df_spark.withColumn('date_created', to_date("dtadfile", 'yyyyMMdd'))
    df_spark = df_spark.withColumn("allowed_stay_days", datediff('allowed_stay_till', 'date_created'))
    df_spark.createOrReplaceTempView('i94')
    query = """SELECT cicid, 
                date_created, 
                i94yr AS year, 
                i94mon AS month,
                i94cit AS citizenship,
                i94res AS resident,
                i94bir AS age,
                biryear AS birth_year,
                gender,
                occup AS occupation,
                allowed_stay_till,
                allowed_stay_days,
                stayed_days,
                i94visa AS visa_class,
                visatype AS visa_type,
                i94port AS port,
                i94mode AS mode,
                i94addr AS arraval_state,
                visapost AS visa_issued_by,
                entdepa AS arrival_flag,
                entdepd AS depart_flag,
                entdepu AS update_flag,
                matflag AS match_flag,
                insnum AS ins_number,
                airline,
                admnum AS admission_number,
                fltno AS flight_number
           FROM i94
       """
    i94 = spark.sql(query)
    i94 = i94.withColumn("cicid", i94["cicid"].cast(IntegerType()))
    i94 = i94.withColumn("year", i94["year"].cast(IntegerType()))
    i94 = i94.withColumn("month", i94["month"].cast(IntegerType()))
    i94 = i94.withColumn("citizenship", i94["citizenship"].cast(IntegerType()))
    i94 = i94.withColumn("resident", i94["resident"].cast(IntegerType()))
    i94 = i94.withColumn("age", i94["age"].cast(IntegerType()))
    i94 = i94.withColumn("birth_year", i94["birth_year"].cast(IntegerType()))
    i94 = i94.withColumn("stayed_days", i94["stayed_days"].cast(IntegerType()))
    i94 = i94.withColumn("visa_class", i94["visa_class"].cast(IntegerType()))
    i94 = i94.withColumn("mode", i94["mode"].cast(IntegerType()))
    i94 = i94.withColumn("admission_number", i94["admission_number"].cast(IntegerType()))
    i94.write.mode('overwrite').partitionBy("month", "year").parquet(output_data + "immigration/")
    print('Immigration data was saved in parquet format on S3')
    

def airport_data():
    '''
    This function creates a parquet file from airport data
    '''
    fname = 'airport-codes_csv.csv'
    airport_df = spark.read.csv(fname, header=True, inferSchema=True).drop_duplicates()
    airport_df.createOrReplaceTempView('airports')
    query = """SELECT ident, 
                      type, 
                      name, 
                      elevation_ft, 
                      iso_country, 
                      iso_region, 
                      municipality, 
                      gps_code, 
                      iata_code AS airport_code, 
                      coordinates
                 FROM airports 
                 WHERE iata_code IS NOT NULL 
                 AND NOT iata_code = 'nan'
                 AND iso_country = 'US'
            """
    airports = spark.sql(query)
    
    def state(iso_region):
        return iso_region.strip().split('-')[-1]
    udf_state = udf(lambda iso_region: state(iso_region), StringType())
    airports = airports.withColumn('state', udf_state('iso_region')).drop('iso_region')
    airports.write.mode('overwrite').parquet(output_data + "airports_data/")
    print('Airport data was saved in parquet format on S3')
    
    
def us_demo_data():
    '''
    This function creates a parquet file from US cities
    '''
    fname = 'us-cities-demographics.csv'
    demo_df = pd.read_csv(fname, ';')
    demo_df = spark.createDataFrame(demo_df)
    demo_df.createOrReplaceTempView('demographics')
    query = """SELECT city, 
                     `Median Age` AS median_age, 
                     `Male Population` AS male_population,
                     `Female Population` AS female_population, 
                     `Total Population` AS population,
                     `Number of Veterans` AS num_veterans, 
                     `Foreign-born` AS foreign_born, 
                     `Average Household Size` AS avg_household_size,
                     `State Code` AS state, 
                     race, 
                     count
                FROM demographics"""
    us_cities_demo = spark.sql(query)
    us_cities_demo = us_cities_demo.withColumn("male_population", us_cities_demo["male_population"].cast(IntegerType()))
    us_cities_demo = us_cities_demo.withColumn("female_population", us_cities_demo["female_population"].cast(IntegerType()))
    us_cities_demo = us_cities_demo.withColumn("num_veterans", us_cities_demo["num_veterans"].cast(IntegerType()))
    us_cities_demo = us_cities_demo.withColumn("foreign_born", us_cities_demo["foreign_born"].cast(IntegerType()))
    us_cities_demo.write.mode('overwrite').parquet(output_data + "demographics/")
    print('US cities data was saved in parquet format on S3')
    

def temp_data():
    '''
    This function creates a parquet file from TemperaturesByCity
    '''
    fname = '/data2/GlobalLandTemperaturesByCity.csv'
    temp_df = spark.read.csv(fname, header=True)
    temp_df = temp_df.withColumn('month', month("dt"))\
            .withColumn('year', year('dt'))\
            .withColumn('day', dayofmonth('dt'))\
            .withColumn('id', monotonically_increasing_id())
    temp_df.createOrReplaceTempView('temperatures')
    query = """SELECT id,
                      AverageTemperature AS avg_temp,
                      AverageTemperatureUncertainty AS sd_temp,
                      LOWER(City) AS city,
                      Country AS country,
                      month,
                      day
                 FROM temperatures
                 WHERE Country = 'United States'
                 AND year = 2012
            """
    temperatures = spark.sql(query)
    temperatures = temperatures.withColumn("avg_temp", temperatures["avg_temp"].cast(FloatType()))
    temperatures = temperatures.withColumn("sd_temp", temperatures["sd_temp"].cast(FloatType()))
    temperatures = temperatures.withColumn("id", temperatures["id"].cast(IntegerType()))
    temperatures.write.parquet(output_data + 'temperatures/', 'overwrite')
    print('Temperature data was saved in parquet format on S3')


def mapping_function(file_name):
    file = open(file_name+'_code.txt', 'r')
    code = []
    name = []
    for i in file:
        row = " ".join(i.split())
        code.append(row[:row.index('=')-1])
        name.append(row[row.index('=')+1:])
    file.close()
    df = pd.DataFrame(list(zip(code,name)), columns = ['code', 'name'])
    df = spark.createDataFrame(df)
    if file_name == 'visas':
        df = df.withColumn("code", df["code"].cast(IntegerType()))
    if file_name == 'modes': 
        df = df.withColumn("code", df["code"].cast(IntegerType()))
    if file_name == 'countries':  
        df = df.withColumn("code", df["code"].cast(IntegerType()))
        df = df.where(col("code").isNotNull())
    df.write.mode('overwrite').parquet(output_data + f"{file_name} + '_code'/")
    print(file_name + ' data was saved in parquet format on S3')

def mapping_function_airport(file_name):
    file = open(file_name+'_code.txt', 'r')
    code = []
    name = []
    for i in file:    
        row = " ".join(i.split())
        code.append(row[:row.index('=')-1])
        row = row.split(",")
        row = row[0].lower()
        row = row[row.index('=')+1:]
        name.append(row)
    file.close()
    df = pd.DataFrame(list(zip(code,name)), columns = ['code', 'name'])
    df = spark.createDataFrame(df)
    df.write.mode('overwrite').parquet(output_data + f"{file_name} +'_code'/")
    print(file_name + ' data was saved in parquet format on S3')



def main():
	output_data = "s3a://udacity-data-engineering-stan/data/"
	config = configparser.ConfigParser()
	config.read('dl.cfg')
	os.environ['AWS_ACCESS_KEY_ID'] = config['AWS CREDS']['AWS_ACCESS_KEY_ID']
	os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS CREDS']['AWS_SECRET_ACCESS_KEY']
    spark=create_spark_session()
    i94_data('jan', 16)
    airport_data()
    us_cities()
    mapping_list=['countries','states','visas','modes']
    for i in mapping_list:
    	mapping_function(i)
    mapping_function_airport('airports')


if __name__ == "__main__":
    main()
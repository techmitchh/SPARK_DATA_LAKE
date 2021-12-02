import configparser
from datetime import datetime
import os
from pyspark.sql.types import TimestampType, DateType
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']
key = os.getenv("AWS_ACCESS_KEY_ID")
secret = os.getenv("AWS_SECRET_ACCESS_KEY")

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark
    

    


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + 'song_data/A/A/A/'
    
    # read song data file
    df = spark.read.load(song_data)

    # extract columns to create songs table
    songs_table = df[['song_id', 'title', 'artist_id', 'year','duration']]
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.save(output_data+'songs', format = 'Parquet', header = True).partitionBy("year", "artist_id")

    # extract columns to create artists table
    artists_table = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]
    
    # write artists table to parquet files
    artists_table.write.save(output_data+'artists', format = 'Parquet', header = True)


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + 'log_data/'

    # read log data file
    df = spark.read.load(log_data)
    
    # filter by actions for song plays
    df = df.select('*').filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    
    # write users table to parquet files
    users_table.write.save(output_data+'users', format = 'Parquet', header = True)

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(int(int(x) / 1000)), TimestampType())
    df = df.select('*', get_timestamp(log_data.ts).alias('start_time'))
    
    # extract columns to create time table
    time_table = df.select(
                        date_format('start_time', 'HH:mm:ss').alias('start_time'),\
                        hour('start_time').alias('hour'),\
                        dayofmonth('start_time').alias('day'),\
                        weekofyear('start_time').alias('week'),\
                        month('start_time').alias('month'),\
                        year('start_time').alias('year'),\
                        dayofweek('start_time').alias('weekday')
                        )
    
    # write time table to parquet files partitioned by year and month
    time_table.write.save(output_data+'time', format = 'Parquet', header = True)

    # read in song data to use for songplays table
    song_df = 

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = 

    # write songplays table to parquet files partitioned by year and month
    songplays_table


def main():
    spark = create_spark_session()
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", key)
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret)
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://techlake/output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

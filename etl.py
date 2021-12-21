import configparser
import pandas as pd
from datetime import datetime
import os
from pyspark.sql.types import TimestampType, DateType
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['IAM']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['IAM']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    spark = SparkSession \
        .builder \
        .appName("Sparkify_Lake") \
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
    songs_table.write.mode('overwrite').partitionBy('year','artist_id').parquet('s3a://techmitch/sparkify_lake/songs.parquet/')

    # extract columns to create artists table
    artists_table = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet("s3a://techmitch/sparkify_lake/artists.parquet")

    return song_data


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
    users_table.write.mode("overwrite").parquet("s3a://techmitch/sparkify_lake/users.parquet")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(int(int(x) / 1000)), TimestampType())
    df = df.select('*', get_timestamp(log_data.ts).alias('start_time'))
    
    # extract columns to create time table
    log_data2 = log_data.select('ts', get_timestamp(log_data.ts).alias('start_time'))
                    
    time_table = log_data2.select(
                        date_format('start_time', 'HH:mm:ss').alias('start_time'),\
                        hour('start_time').alias('hour'),\
                        dayofmonth('start_time').alias('day'),\
                        weekofyear('start_time').alias('week'),\
                        month('start_time').alias('month'),\
                        year('start_time').alias('year'),\
                        dayofweek('start_time').alias('weekday')
                        )
    time_table.write.partitionBy('year','month').mode("overwrite").parquet("s3a://techmitch/sparkify_lake/time.parquet") # write time table to parquet files partitioned by year and month


    # read in song data to use for songplays table
    song_df = input_data + 'song_data/A/A/A/'

    # extract columns from joined song and log datasets to create songplays table 
    log_data.createOrReplaceTempView("log_data_table")
    songplays_table = song_df.createOrReplaceTempView("song_data_table")

    # converts time column to seperate table to then join to song_data table
    logpd = log_data.toPandas()
    logpd['ts'] = pd.to_datetime(logpd['ts'], unit='ms')
    log = logpd[['ts','artist']]
    pdto_spark = spark.createDataFrame(log)
    pdto_spark.createOrReplaceTempView("log_time_table")

    # Extracts data and joins data from log_time table and song_data table to create songplays table and then writes to parquet
    songplays = spark.sql('''
                        SELECT st.ts as start_time,
                               l.userid as user_id,
                               level,
                               s.song_id,
                               s.artist_id,
                               l.sessionId,
                               l.location,
                               l.userAgent as user_agent,
                               DATE_PART('year', st.ts) as year,
                               DATE_PART('month', st.ts) as month
                          FROM log_data_table as l
                          LEFT JOIN song_data_table as s
                            ON l.artist = s.artist_name
                          LEFT JOIN log_time_table as st
                            ON l.artist = st.artist
                         WHERE l.page = "NextSong"
                      ORDER BY song_id DESC
                      ''').write.partitionBy('year','month').mode("overwrite").parquet("s3a://techmitch/sparkify_lake/songplay.parquet") # write songplays table to parquet files partitioned by year and month

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://techlake/output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

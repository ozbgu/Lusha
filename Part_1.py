from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql import *


# initiate a session of spark
spark=SparkSession.builder.appName("test").config(
    "spark.driver.extraJavaOptions",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED",
).getOrCreate()

def create_session(appname):
    spark_session = SparkSession.builder.appName(appname).master('yarn').config("hive.metastore.uris", "thrift://uds-far-mn1.dab.02.net:9083").enableHiveSupport().getOrCreate()
    return spark_session

if __name__ == '__main__':
    spark_session = create_session('Lusha')



# extract csv and parquet files
zones=spark.read.csv('/Users/oz.barlev/Downloads/lusha-de-interview/data_part1/yellow_taxi_jan_25_2018/taxi_zones.csv',inferSchema=True,header=True)
rides=spark.read.parquet('/Users/oz.barlev/Downloads/lusha-de-interview/data_part1/yellow_taxi_jan_25_2018/*.parquet')


# q1
print (' ############################## Question 1 ############################## ')
# here the sum is calculated per passenger, the calculation is for both pickups/dropoffs
# showing the 3 highest Borough and the number of passengers
join_tables=rides.join(zones,zones.LocationID ==  rides.PULocationID,"inner")
join_tables.groupBy('Borough') \
    .agg({"passenger_count": "sum"}) \
    .withColumnRenamed("sum(passenger_count)", "num_of_passengers")\
    .sort(desc("num_of_passengers")).show(3) 


# q2
print (' ############################## Question 2 ############################## ')
# adding a field named hour, holding the number of pickups per each hour
rides=rides.withColumn("hour", lit(hour(F.to_timestamp("tpep_pickup_datetime","dd/MM/yyyy HH:mm:ss"))))
#creating a view to make it easier to query
rides.createTempView("rides")
spark.sql("SELECT hour,count(*) as num_of_rides_per_hour FROM rides group by 1 order by 2 desc").show(3)



# q3
print (' ############################## Question 3 ############################## ')
#assuming that long trip is longer then 3km
spark.sql("SELECT hour as long_trip_peak_hours,count(passenger_count)  as num_of_rides FROM rides where trip_distance>3 group by 1 order by 2 desc").show(3)
spark.sql("SELECT hour as short_trip_peak_hours,count(passenger_count)  as num_of_rides FROM rides where trip_distance<3 group by 1 order by 2 desc").show(3)


# in order to see the whole picture, this table is shoing the comparssion between short and long dist for the same hour
spark.sql("SELECT hour,sum(case when trip_distance>3 then 1 else 0 end) as num_of_long_rides,sum(case when trip_distance<3 then 1 else 0 end) as num_of_short_rides \
           FROM rides group by 1 ").show(24)



# q4
print (' ############################## Question 4 ############################## ')
# # assuming that long trip is longer then 3km and short is less then 
spark.sql("with short_rides as (                                        \
    SELECT distinct payment_type FROM rides where trip_distance<3),     \
    long_rides as (                                                     \
    SELECT distinct payment_type FROM rides where trip_distance>3)      \
                                                                        \
    select 'short_rides' as ride_dist, payment_type from short_rides    \
    union                                                               \
    select 'long_rides' as ride_dist,payment_type  from long_rides      \
    ").show()



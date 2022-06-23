from pathlib import Path
import pandas as pd
import pandasql as ps


# reading the parquet files and concat all to one big table
data_dir = Path('/Users/oz.barlev/Downloads/lusha-de-interview/data_part1/yellow_taxi_jan_25_2018')
rides = pd.concat(
    pd.read_parquet(parquet_file)
    for parquet_file in data_dir.glob('*.parquet')
)

# extract csv file - zones table
zones= pd.read_csv('/Users/oz.barlev/Downloads/lusha-de-interview/data_part1/yellow_taxi_jan_25_2018/taxi_zones.csv')



#q1
print (' ############################## Question 1 ############################## ')
# here the sum is calculated per passenger, the calculation is for both pickups/dropoffs
# showing the 3 highest Borough and the number of passengers
q1 = """SELECT z.Borough,sum(passenger_count) as number_of_passenger  
        FROM rides r join zones z on r.PULocationID=z.LocationID group by 1 order by 2 desc limit 3"""
print(ps.sqldf(q1, locals()))


# #q2
print (' ############################## Question 2 ############################## ')
# # adding a field named hours, holding the hours per each pickup_datetime
rides['hours'] = pd.to_datetime(rides['tpep_pickup_datetime'].apply(lambda x : pd.to_datetime(str(x)))).dt.hour
# performing the query
q2 = """SELECT hours,count(*) FROM rides group by 1 order by 2 desc limit 1"""
print(ps.sqldf(q2, locals()))


#q3
print (' ############################## Question 3 ############################## ')
#assuming that long trip is longer then 5km
q3_1 = """SELECT hours as long_trip_peak_hours,count(passenger_count)  as num_of_rides FROM rides where trip_distance>5 group by 1 order by 2 desc limit 3"""
q3_2 = """SELECT hours as short_trip_peak_hours,count(passenger_count)  as num_of_rides FROM rides where trip_distance<5 group by 1 order by 2 desc limit 3"""
print(ps.sqldf(q3_1, locals()))
print(ps.sqldf(q3_2, locals()))

# in order to see the whole picture, this table is shoing the comparssion between short and long dist for the same hour
q3_2 = """SELECT hours,sum(case when trip_distance>5 then 1 else 0 end)  as num_of_long_rides,sum(case when trip_distance<5 then 1 else 0 end) as num_of_short_rides 
          FROM rides group by 1 """
print(ps.sqldf(q3_2, locals()))


#q4
print (' ############################## Question 4 ############################## ')
# # assuming that long trip is longer then 5km and short is less then 
q4 = """
    with short_rides as (
    SELECT distinct payment_type FROM rides where trip_distance<5),
    long_rides as (
    SELECT distinct payment_type FROM rides where trip_distance>5)

    select 'short_rides' as ride_dist, payment_type from short_rides 
    union
    select 'long_rides' as ride_dist,payment_type  from long_rides 
    """
print(ps.sqldf(q4, locals()))





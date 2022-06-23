
## Lusha Data Engineer coding assignment

###  Part 1 (python/ pyspark)

As a Data engineer you are asked to analyze a data set and uncover useful insights.
In order to achieve this you need to load **csv and parquet files from the "data_part1/yellow_taxi_jan_25_2018" folder**, and answer a few questions:
    
   1. Which Borough have the most pickups/dropoffs overall?
   2. What are the peak hours for taxi?
   3. What are the peak hours for long/short trips?
   5. How are people paying for the ride, on long/short trips?

------------


###  Part 2 (sql) 

The Marketing team would like to track their activities and understand which traffic source shows the highest performance. The traffic sources are reported via UTMs.

**Marketing team's KPIs:**

Registrations of new users<br/>
Billing out of users’ purchases

*The logic of attribution is as follows:*

**Registrations:** First Touch - the first UTM source the user encountered

**Billing from purchase:** Time Decay - 50% of the billing is attributed to the last UTM the user encountered before purchase, the rest of the money is distributed evenly between the prior UTMs the user encountered

*How is the real data reflected in tables?*


Each time a user visits the company’s website, his visit and the UTMs in the URL are documented in the users_utm table ("users_utm" file from the "data" folder).<br/> Each user registration is documented in the users table ("users" file from the "data" folder).<br/>
Each purchase is documented in the purchases table ("purchases" file from the "data" folder).<br/>


**Instructions for the assignment:**<br/> Produce a SQL ETL script to populate aggregated table with the following structure:


CalendarDate date<br/>utmSource varchar(100)<br/>number_of_registrations int<br/>number_of_purchases int<br/>total_billing decimal (12,6)




------------

**Good luck!**


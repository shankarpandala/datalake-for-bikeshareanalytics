# Databricks notebook source
# MAGIC %md
# MAGIC # Riders's Dimension Table

# COMMAND ----------

dim_rider = spark.sql('''
    SELECT 
        rider_id, first_name, last_name, address, birthday, start_date, end_date, is_member
    FROM riders
''')

# COMMAND ----------

dim_rider.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("dim_rider")

# COMMAND ----------

# MAGIC %md
# MAGIC # Stations's Dimension Table

# COMMAND ----------

dim_station = spark.sql('''
    SELECT station_id, name, latitude , longitude FROM stations
''')

# COMMAND ----------

dim_station.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("dim_station")

# COMMAND ----------

# MAGIC %md
# MAGIC # Dimension Table for Time

# COMMAND ----------

min_date_qry = spark.sql('''
    SELECT MIN(started_at) as started_at FROM trips
''')
min_date = min_date_qry.first().asDict()['started_at']

max_date_qry = spark.sql('''
    SELECT DATEADD(year, 5, MAX(started_at)) as started_at FROM trips
''')
max_date = max_date_qry.first().asDict()['started_at']

expression = f"sequence(to_date('{min_date}'), to_date('{max_date}'), interval 1 day)"

# COMMAND ----------

from pyspark.sql import functions as f
from pyspark.sql.types import StringType


dim_time = spark.createDataFrame([(1,)], ["time_id"])

dim_time = dim_time.withColumn("dateinit", f.explode(f.expr(expression)))
dim_time = dim_time.withColumn("date", f.to_timestamp(dim_time.dateinit, "yyyy-MM-dd"))
dim_time = dim_time \
            .withColumn("dayofweek", f.dayofweek(dim_time.date)) \
            .withColumn("dayofmonth", f.dayofmonth(dim_time.date)) \
            .withColumn("weekofyear", f.weekofyear(dim_time.date)) \
            .withColumn("year", f.year(dim_time.date)) \
            .withColumn("quarter", f.quarter(dim_time.date)) \
            .withColumn("month", f.month(dim_time.date)) \
            .withColumn("time_id", dim_time.date.cast(StringType())) \
            .drop(f.col("dateinit"))

# COMMAND ----------

# display(dim_time)

# COMMAND ----------

dim_time.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("dim_time")

# COMMAND ----------

# MAGIC %md
# MAGIC # Fact Table for Trips

# COMMAND ----------

fact_trip = spark.sql('''
    SELECT 
        trips.trip_id,
        riders.rider_id,
        trips.start_station_id, 
        trips.end_station_id, 
        start_time.time_id                                                  AS start_time_id,
        end_time.time_id                                                    AS end_time_id,
        trips.rideable_type,
        DATEDIFF(hour, trips.started_at, trips.ended_at)                    AS duration,
        DATEDIFF(year, riders.birthday, trips.started_at)                     AS rider_age

    FROM trips
    JOIN riders                     ON riders.rider_id = trips.rider_id
    JOIN dim_time AS start_time     ON start_time.date = trips.started_at
    JOIN dim_time AS end_time       ON end_time.date = trips.ended_at
''')

# COMMAND ----------

display(fact_trip)

# COMMAND ----------

fact_trip.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("fact_trip")

# COMMAND ----------

# MAGIC %md
# MAGIC # Fact Table for Payments

# COMMAND ----------

fact_payment = spark.sql('''
    SELECT
        payment_id,
        payments.amount,
        payments.rider_id,
        dim_time.time_id
    FROM payments
    JOIN dim_time ON dim_time.date = payments.date
''')

# COMMAND ----------

fact_payment.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("fact_payment")

# COMMAND ----------



# Databricks notebook source
# MAGIC %md
# MAGIC #Business Outcomes

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Analyze how much time is spent per ride
# MAGIC - Based on date and time factors such as day of week and time of day
# MAGIC - Based on which station is the starting and / or ending station
# MAGIC - Based on age of the rider at time of the ride
# MAGIC - Based on whether the rider is a member or a casual rider

# COMMAND ----------

time_spent = spark.sql('''
    SELECT 
        day(trips.started_at) as _day,
        avg(unix_timestamp(trips.ended_at, 'yyyy-MM-dd HH:mm:ss') - unix_timestamp(trips.started_at, 'yyyy-MM-dd HH:mm:ss'))/60 as duration_mins
    FROM trips 
    GROUP BY _day
    ORDER BY _day
''')

# COMMAND ----------

display(time_spent)

# COMMAND ----------

time_weekday = spark.sql('''
    SELECT 
        dayOfWeek(trips.started_at) as Weekday,
        avg(unix_timestamp(trips.ended_at, 'yyyy-MM-dd HH:mm:ss') - unix_timestamp(trips.started_at, 'yyyy-MM-dd HH:mm:ss'))/60 as duration_mins
    FROM trips 
    GROUP BY Weekday
    ORDER BY Weekday
''')

# COMMAND ----------

display(time_weekday)

# COMMAND ----------



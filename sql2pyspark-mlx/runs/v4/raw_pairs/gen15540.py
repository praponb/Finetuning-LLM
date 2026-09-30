from pyspark.sql import functions as F

result = df_web_events.select("event_time", F.to_date("event_time").alias("event_time_date"))

from pyspark.sql import functions as F

result = df_web_events.select("duration_sec", "event_type")

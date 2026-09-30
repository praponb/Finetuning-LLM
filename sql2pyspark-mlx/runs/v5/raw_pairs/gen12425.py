from pyspark.sql import functions as F

result = df_web_events.select("id", "duration_sec", "page", "event_type")

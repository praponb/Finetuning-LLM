from pyspark.sql import functions as F

result = df_web_events.select("id", "session_id", "duration_sec")

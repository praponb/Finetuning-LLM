from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec").between(126, 320))
    .select("event_time", F.hour("event_time").alias("event_time_hour"))
)

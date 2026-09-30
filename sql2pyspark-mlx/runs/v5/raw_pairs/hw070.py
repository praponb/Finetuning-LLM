from pyspark.sql import functions as F

result = df_web_events.select(
    "id",
    F.hour("event_time").alias("hr"),
    F.minute("event_time").alias("mn"),
    F.second("event_time").alias("sc"),
)

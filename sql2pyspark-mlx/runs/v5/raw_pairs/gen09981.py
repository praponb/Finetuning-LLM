from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([17, 31]) & F.col("page").isin(["/home", "/cart"]))
    .agg(
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.min("event_time").alias("first_event_time"),
    )
)

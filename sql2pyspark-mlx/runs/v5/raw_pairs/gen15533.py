from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([6, 24, 29, 32]))
    .select(
        "id",
        "duration_sec",
        F.date_format("event_time", "yyyy-MM").alias("event_time_label"),
    )
    .orderBy("event_time_label")
)

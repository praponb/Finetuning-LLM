from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type").isin(["page_view", "click"]) & (F.col("duration_sec") < 341))
    .select(
        "id",
        "session_id",
        F.date_format("event_time", "yyyy-MM").alias("event_time_label"),
    )
)

from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([14, 25, 34]) & F.col("session_id").isin([18, 32]) & F.col("event_type").isin(["click", "page_view"]))
    .select(
        "id",
        "page",
        "session_id",
        "event_time",
        F.year("event_time").alias("event_time_year"),
    )
)

from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("event_type") == "page_view") & F.col("session_id").isin([18, 35]) & F.col("session_id").isin([4, 10, 12]))
    .select(
        "page",
        F.length("event_type").alias("event_type_length"),
        F.datediff(F.current_date(), "event_time").alias("days_since_event_time"),
    )
    .orderBy(F.col("days_since_event_time").desc(), "event_type_length")
)

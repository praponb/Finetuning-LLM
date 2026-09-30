from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("duration_sec") <= 248) & F.col("session_id").isin([21, 22, 28, 39]) & (F.col("event_type") == "page_view"))
    .select(
        "id",
        "session_id",
        F.concat_ws("-", "page", "event_type").alias("page_event_type_key"),
    )
)

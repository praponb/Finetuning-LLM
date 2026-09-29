from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("duration_sec") >= 241) & (F.col("event_type") == "search") & (F.col("session_id") == 37))
    .select(
        "id",
        "event_type",
        "page",
        "session_id",
        F.concat_ws("-", "page", "event_type").alias("page_event_type_key"),
    )
)

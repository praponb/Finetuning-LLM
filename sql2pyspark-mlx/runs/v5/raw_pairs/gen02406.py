from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([3, 30, 39]) | (F.col("event_time") < "2023-12-01"))
    .select(
        "id",
        "session_id",
        "event_type",
        "duration_sec",
        F.concat_ws("-", "event_type", "page").alias("event_type_page_key"),
    )
)

from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("duration_sec") > 325) & (F.col("session_id") == 13))
    .select(
        "id",
        "event_time",
        "session_id",
        F.concat("page", F.lit(" - "), "event_type").alias("page_event_type"),
    )
    .orderBy(F.col("id").desc(), "page_event_type")
)

from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([8, 34]))
    .select(
        "id",
        "event_type",
        "duration_sec",
        F.concat_ws(",", "event_type", "page").alias("event_type_page_key"),
    )
    .orderBy(F.col("duration_sec").desc(), "id")
    .limit(1)
)

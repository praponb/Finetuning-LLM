from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec").between(127, 447))
    .select(
        "id",
        "event_time",
        "page",
        "event_type",
        F.substring("page", 1, 4).alias("page_prefix"),
    )
    .orderBy("event_type")
)

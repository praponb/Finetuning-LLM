from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type") == "add_to_cart")
    .select(
        "id",
        "event_type",
        "event_time",
        "duration_sec",
        F.when(F.col("event_type") == "page_view", 1).otherwise(0).alias("is_page_view"),
    )
    .orderBy(F.col("duration_sec").desc())
)

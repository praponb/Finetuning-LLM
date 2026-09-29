from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page") == "/product")
    .select(
        "id",
        "duration_sec",
        "event_time",
        "event_type",
        F.coalesce("page", F.lit("Unknown")).alias("page_filled"),
    )
)

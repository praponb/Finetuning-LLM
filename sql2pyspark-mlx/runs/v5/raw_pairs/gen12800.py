from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("event_type") == "page_view") & (F.col("page") == "/cart"))
    .select(
        "page",
        "event_time",
        "event_type",
        F.date_format("event_time", "MMM yyyy").alias("event_time_label"),
        F.coalesce("duration_sec", F.lit(0)).alias("duration_sec_filled"),
    )
)

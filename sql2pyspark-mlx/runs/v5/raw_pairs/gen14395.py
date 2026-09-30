from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("duration_sec") >= 142) & ((F.col("duration_sec") == 353) | F.col("event_type").isin(["search", "page_view", "purchase"])))
    .select(
        "id",
        "event_time",
        "duration_sec",
        F.to_date("event_time").alias("event_time_date"),
    )
)

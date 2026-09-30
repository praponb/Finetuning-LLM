from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(~F.col("event_type").isin(["page_view", "add_to_cart"]))
    .groupBy("event_type")
    .agg(
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
        F.min("duration_sec").alias("min_duration_sec"),
    )
    .filter(F.col("min_duration_sec") >= 270)
    .orderBy(F.col("min_duration_sec").desc(), "event_type")
)

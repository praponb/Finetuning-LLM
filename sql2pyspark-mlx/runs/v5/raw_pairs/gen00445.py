from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type").isin(["add_to_cart", "click"]) & F.hour("event_time").between(8, 14))
    .groupBy("page")
    .agg(
        F.count("duration_sec").alias("duration_sec_count"),
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
    )
    .filter(F.col("duration_sec_count") < 4)
)

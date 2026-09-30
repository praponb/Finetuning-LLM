from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page").isin(["/cart", "/search", "/checkout"]) | F.col("event_type").isin(["search", "purchase", "add_to_cart"]))
    .groupBy(F.month("event_time").alias("event_month"), "event_type")
    .agg(
        F.countDistinct("duration_sec").alias("unique_event_types"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
    )
)

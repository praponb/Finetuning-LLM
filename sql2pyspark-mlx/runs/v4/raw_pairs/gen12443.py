from pyspark.sql import functions as F

recent_web_events = (
    df_web_events
    .filter(F.col("event_time") < "2025-03-30")
    .withColumn("duration_sec_rounded", F.round("duration_sec", 2))
)

web_events_stats = (
    recent_web_events
    .groupBy("event_type", "page")
    .agg(
        F.sum(F.when(F.col("page") == "/product", F.col("duration_sec_rounded")).otherwise(0)).alias("product_duration_sec_rounded"),
    )
)

result = web_events_stats.orderBy(F.col("product_duration_sec_rounded").desc())

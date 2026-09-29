from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_time") < "2024-07-30")
    .groupBy(F.date_format("event_time", "yyyy-MM").alias("event_month"), "page")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0))) / F.count("*")), 1).alias("pct_add_to_cart"),
    )
    .filter(F.col("pct_add_to_cart") >= 67)
    .orderBy("event_month", "page")
    .limit(5)
)

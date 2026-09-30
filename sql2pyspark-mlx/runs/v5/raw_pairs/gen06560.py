from pyspark.sql import functions as F

valid_web_events = (
    df_web_events
    .filter((F.col("page") == "/search") & (F.to_date("event_time") < "2023-12-29"))
)

summary = (
    valid_web_events
    .groupBy(F.quarter("event_time").alias("event_quarter"))
    .agg(
        F.sum(F.when(F.col("page") == "/cart", 1).otherwise(0)).alias("cart_count"),
        F.max("duration_sec").alias("max_duration_sec"),
    )
)

result = summary.select("event_quarter", "cart_count", "max_duration_sec")

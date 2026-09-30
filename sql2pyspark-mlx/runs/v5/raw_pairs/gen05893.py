from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(F.date_trunc("MONTH", "event_time").alias("event_month"))
    .agg(
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
        F.sum(F.when(F.col("page") == "/cart", 1).otherwise(0)).alias("cart_count"),
    )
    .orderBy(F.col("cart_count").desc(), "event_month")
    .limit(10)
)

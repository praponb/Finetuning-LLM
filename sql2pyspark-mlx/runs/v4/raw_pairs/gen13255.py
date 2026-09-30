from pyspark.sql import functions as F

result = (
    df_web_events
    .withColumn("duration_sec_tier", F.when(F.col("duration_sec") >= 453, "LARGE").otherwise("SMALL"))
    .rollup("page", "duration_sec_tier")
    .agg(
        F.avg(F.when(F.col("page") == "/cart", F.col("duration_sec"))).alias("cart_avg_duration_sec"),
        F.count(F.when(F.col("event_type") == "purchase", 1)).alias("purchase_count"),
        F.avg("duration_sec").alias("avg_duration_sec"),
        F.grouping_id().alias("gid"),
    )
    .filter(F.col("cart_avg_duration_sec") > 0)
)

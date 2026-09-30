from pyspark.sql import functions as F

filtered_orders = (
    df_orders
    .filter((F.col("store_id") == 8) & F.col("store_id").isNotNull())
    .withColumn("amount_rounded", F.round("amount", 0))
)

orders_stats = (
    filtered_orders
    .groupBy("channel")
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
)

result = (
    orders_stats
    .select("channel", "avg_amount", "status_values")
    .orderBy(F.col("avg_amount").desc(), "channel")
    .limit(3)
)

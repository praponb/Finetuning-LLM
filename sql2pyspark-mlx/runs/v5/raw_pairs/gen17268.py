from pyspark.sql import functions as F

src = (
    df_orders
    .filter((F.col("amount") > 1300) & F.col("customer_id").isNull() & (F.col("discount") < 0.2))
)

orders_stats = (
    src
    .groupBy(
        F.when(F.col("channel").isin(["mobile", "store"]), "core").otherwise("other").alias("channel_group"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("channel") == "store", 1).otherwise(0))) / F.count("*")), 1).alias("pct_store"),
        F.min("discount").alias("min_discount"),
    )
)

result = (
    orders_stats
    .filter(F.col("pct_store") > 24)
    .select("channel_group", "min_discount")
)

from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("orders.store_id"), "inner")
    .filter(F.col("stores.city").isin(["London", "Bangkok"]))
    .groupBy(
        F.when(F.col("stores.region").isin(["North", "West"]), "tier_1").otherwise("other").alias("region_group"),
    )
    .agg(
        F.count("*").alias("order_count"),
        F.round(F.stddev(F.col("orders.discount")), 2).alias("stddev_discount"),
    )
)

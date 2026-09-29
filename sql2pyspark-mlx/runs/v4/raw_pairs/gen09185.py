from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status") == "SHIPPED")
    .groupBy("store_id")
    .agg(
        F.round(F.avg("discount"), 2).alias("avg_discount"),
        F.round(((100 * F.sum(F.when(F.col("status") == "COMPLETED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_completed"),
        F.min("shipping_fee").alias("min_shipping_fee"),
    )
    .filter(F.col("min_shipping_fee") >= 22)
    .drop("min_shipping_fee")
    .orderBy(F.col("store_id").desc())
)

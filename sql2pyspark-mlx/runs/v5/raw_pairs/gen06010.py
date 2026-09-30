from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isin([3, 6, 20]))
    .groupBy(F.month("order_date").alias("order_month"))
    .agg(
        F.count("shipping_fee").alias("shipping_fee_count"),
        F.round(((100 * F.sum(F.when(F.col("status") == "PENDING", 1).otherwise(0))) / F.count("*")), 1).alias("pct_pending"),
    )
    .orderBy("order_month", F.col("pct_pending").desc())
)

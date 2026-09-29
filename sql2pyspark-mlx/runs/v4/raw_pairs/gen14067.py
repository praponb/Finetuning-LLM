from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("order_date").between("2023-10-05", "2025-05-01") & F.col("amount").between(1590, 1600))
    .agg(
        F.count(F.when(F.col("status") == "PENDING", 1)).alias("pending_count"),
        F.round(F.stddev("shipping_fee"), 2).alias("stddev_shipping_fee"),
        F.round(((100 * F.sum(F.when(F.col("channel") == "mobile", 1).otherwise(0))) / F.count("*")), 1).alias("pct_mobile"),
    )
)

from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel")
    .agg(
        F.round(F.stddev("shipping_fee"), 2).alias("stddev_shipping_fee"),
        F.round(((100 * F.sum(F.when(F.col("status") == "RETURNED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_returned"),
    )
    .orderBy("channel", "pct_returned")
)

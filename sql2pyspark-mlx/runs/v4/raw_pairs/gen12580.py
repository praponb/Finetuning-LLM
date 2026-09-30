from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel") == "partner")
    .agg(
        F.round(F.avg("discount"), 2).alias("avg_discount"),
        F.sum(F.when(F.col("channel") == "web", F.col("shipping_fee")).otherwise(0)).alias("web_shipping_fee"),
        F.round(((100 * F.sum(F.when(F.col("channel") == "store", 1).otherwise(0))) / F.count("*")), 1).alias("pct_store"),
    )
)

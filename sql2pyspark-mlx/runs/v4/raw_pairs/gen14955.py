from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(
        F.when(F.col("shipping_fee") >= 41, "high").when(F.col("shipping_fee") >= 14, "mid").otherwise("low").alias("shipping_fee_bucket"),
    )
    .agg(
        F.max("shipping_fee").alias("max_shipping_fee"),
        F.count(F.when(F.col("channel") == "mobile", 1)).alias("mobile_count"),
    )
    .filter(F.col("max_shipping_fee") > 18)
)

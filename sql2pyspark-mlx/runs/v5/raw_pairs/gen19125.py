from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("discount") > 0.3) & F.col("discount").between(0.1, 0.24) & ~F.col("channel").isin(["mobile", "partner", "web"]))
    .groupBy(
        F.when(F.col("shipping_fee") >= 33, "high").when(F.col("shipping_fee") >= 13, "mid").otherwise("low").alias("shipping_fee_bucket"),
    )
    .agg(
        F.min("shipping_fee").alias("min_shipping_fee"),
        F.count("discount").alias("discount_count"),
        F.max("amount").alias("max_amount"),
    )
    .filter(F.col("discount_count") < 2)
)

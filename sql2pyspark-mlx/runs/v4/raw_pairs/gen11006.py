from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("channel") == "partner") & ((F.col("amount") >= 880) | F.col("customer_id").isNull()))
    .groupBy(
        F.when(F.col("discount") >= 0.4, "high").when(F.col("discount") >= 0.12, "mid").otherwise("low").alias("discount_bucket"),
    )
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.sum("shipping_fee").alias("total_shipping_fee"),
    )
)

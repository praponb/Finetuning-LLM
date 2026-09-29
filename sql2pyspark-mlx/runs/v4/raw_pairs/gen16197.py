from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(
        F.when(F.col("discount") >= 0.3, "high").when(F.col("discount") >= 0.15, "mid").otherwise("low").alias("discount_bucket"),
    )
    .agg(F.sum("shipping_fee").alias("total_shipping_fee"))
    .orderBy("discount_bucket")
    .limit(3)
)

from pyspark.sql import functions as F

base = (
    df_orders
    .filter(F.col("store_id").isNotNull() & (F.col("customer_id") == 21))
    .withColumn("amount_plus_discount", (F.col("amount") + F.col("discount")))
)

orders_stats = (
    base
    .groupBy(
        F.when(F.col("shipping_fee") >= 40, "high").when(F.col("shipping_fee") >= 13, "mid").otherwise("low").alias("shipping_fee_bucket"),
    )
    .agg(F.min("discount").alias("min_discount"))
)

result = orders_stats.select("shipping_fee_bucket", "min_discount")

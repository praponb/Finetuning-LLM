from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee").between(32, 38))
    .groupBy(
        "customer_id",
        F.when(F.col("amount") >= 1740, "high").when(F.col("amount") >= 770, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.round(F.avg("discount"), 2).alias("avg_discount"),
        F.min("order_date").alias("first_order_date"),
    )
    .orderBy(F.col("avg_discount").desc(), "customer_id", "amount_bucket")
    .limit(10)
)

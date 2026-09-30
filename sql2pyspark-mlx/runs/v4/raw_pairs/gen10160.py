from pyspark.sql import functions as F

filtered_orders = (
    df_orders
    .filter((F.col("order_date") >= "2023-03-01") & ((F.col("discount") >= 0.16) | F.col("customer_id").isNull()))
)

agg = (
    filtered_orders
    .groupBy(
        F.when(F.col("amount") >= 1660, "high").when(F.col("amount") >= 320, "mid").otherwise("low").alias("amount_bucket"),
        "status",
    )
    .agg(
        F.avg("shipping_fee").alias("avg_shipping_fee"),
        F.sum("shipping_fee").alias("total_shipping_fee"),
    )
)

result = (
    agg
    .select("amount_bucket", "status", "avg_shipping_fee", "total_shipping_fee")
    .orderBy("amount_bucket", "status")
    .limit(5)
)

from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee") <= 22)
    .groupBy(
        F.when(F.col("amount") >= 1660, "high").when(F.col("amount") >= 780, "mid").otherwise("low").alias("amount_bucket"),
        F.date_trunc("MONTH", "order_date").alias("order_month"),
    )
    .agg(
        F.sum(F.when(F.col("channel") == "web", 1).otherwise(0)).alias("web_count"),
        F.round(F.avg("shipping_fee"), 2).alias("avg_shipping_fee"),
    )
    .filter(F.col("web_count") >= 34)
    .select("amount_bucket", "order_month", "web_count", "avg_shipping_fee")
    .orderBy(F.col("avg_shipping_fee").desc())
)

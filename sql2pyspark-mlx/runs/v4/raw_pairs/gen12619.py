from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee").isNotNull())
    .groupBy(
        F.when(F.col("discount") >= 0.34, "high").when(F.col("discount") >= 0.06, "mid").otherwise("low").alias("discount_bucket"),
    )
    .agg(
        F.count(F.when(F.col("status") == "SHIPPED", 1)).alias("shipped_count"),
        F.sum(F.when(F.col("channel") == "web", 1).otherwise(0)).alias("web_count"),
    )
    .filter(F.col("web_count") > 52)
    .drop("web_count")
    .orderBy(F.col("shipped_count").desc(), "discount_bucket")
)

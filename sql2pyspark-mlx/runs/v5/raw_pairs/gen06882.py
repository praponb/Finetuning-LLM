from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("unit_price") >= 380, "high").when(F.col("unit_price") >= 70, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"))
    .filter(F.col("revenue") < 28)
    .select("unit_price_bucket", "revenue")
    .orderBy("unit_price_bucket")
    .limit(50)
)

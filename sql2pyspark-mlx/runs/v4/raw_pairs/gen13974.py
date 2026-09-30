from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("unit_price") >= 430, "high").when(F.col("unit_price") >= 110, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(F.avg("quantity").alias("avg_quantity"))
    .orderBy("unit_price_bucket")
    .limit(10)
)

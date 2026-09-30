from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("unit_price") >= 330, "high").when(F.col("unit_price") >= 150, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.avg("unit_price").alias("avg_unit_price"),
    )
    .orderBy("unit_price_bucket", F.col("total_quantity").desc())
)

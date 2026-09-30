from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("order_id").isin([27, 29]) & (F.col("unit_price") >= 210)) | (F.col("product_id") == 4))
    .groupBy(
        F.when(F.col("unit_price") >= 420, "high").when(F.col("unit_price") >= 140, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(F.round(F.avg("quantity"), 2).alias("avg_quantity"))
)

from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("unit_price").between(220, 300) & F.col("unit_price").between(180, 250)) | (F.col("quantity") == 8))
    .groupBy(
        F.when(F.col("quantity") >= 7, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.round(F.avg("quantity"), 2).alias("avg_quantity"))
    .orderBy("quantity_bucket")
)

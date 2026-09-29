from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([3, 7, 9, 39]) & F.col("order_id").isin([11, 16, 22, 38]) & (F.col("quantity") >= 4))
    .groupBy(
        F.when(F.col("quantity") >= 8, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.min("quantity").alias("min_quantity"))
    .filter(F.col("min_quantity") > 7.0)
    .orderBy(F.col("quantity_bucket").desc())
)

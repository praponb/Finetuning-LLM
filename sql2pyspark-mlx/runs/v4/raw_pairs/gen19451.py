from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("quantity") >= 8, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.max("quantity").alias("max_quantity"),
        F.min("unit_price").alias("min_unit_price"),
    )
    .filter(F.col("avg_quantity") < 3.0)
    .orderBy(F.col("min_unit_price").desc())
)

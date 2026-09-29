from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isin([9, 22, 38]))
    .groupBy(
        F.when(F.col("quantity") >= 8, "high").when(F.col("quantity") >= 2, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"))
    .filter(F.col("revenue") >= 47)
    .orderBy(F.col("quantity_bucket").desc())
)

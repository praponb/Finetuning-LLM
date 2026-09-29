from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([4, 7, 31, 39]) & F.col("product_id").isin([16, 31, 33, 36]))
    .groupBy(
        F.when(F.col("quantity") >= 8, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"))
    .filter(F.col("revenue") >= 21)
)

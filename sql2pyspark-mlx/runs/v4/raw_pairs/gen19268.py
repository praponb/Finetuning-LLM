from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isin([27, 33, 40]) & F.col("order_id").isin([13, 19, 35, 39]))
    .groupBy(
        "order_id",
        F.when(F.col("quantity") >= 9, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.count("quantity").alias("quantity_count"))
)

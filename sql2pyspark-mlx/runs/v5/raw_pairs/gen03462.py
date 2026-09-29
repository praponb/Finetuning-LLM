from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("quantity") >= 9, "high").when(F.col("quantity") >= 2, "mid").otherwise("low").alias("quantity_bucket"),
        "product_id",
    )
    .agg(F.min("quantity").alias("min_quantity"))
    .orderBy(F.col("product_id").desc(), "quantity_bucket")
    .limit(3)
)

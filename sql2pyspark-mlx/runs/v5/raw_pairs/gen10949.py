from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("quantity") >= 7, "high").when(F.col("quantity") >= 4, "mid").otherwise("low").alias("quantity_bucket"),
        "product_id",
    )
    .agg(F.min("quantity").alias("min_quantity"))
    .orderBy("product_id")
)

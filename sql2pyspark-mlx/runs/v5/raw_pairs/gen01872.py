from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        "product_id",
        F.when(F.col("quantity") >= 7, "high").when(F.col("quantity") >= 4, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.count("quantity").alias("quantity_count"))
    .filter(F.col("quantity_count") > 5)
    .select("product_id", "quantity_bucket", "quantity_count")
)

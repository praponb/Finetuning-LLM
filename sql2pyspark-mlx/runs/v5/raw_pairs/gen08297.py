from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("quantity") >= 9, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.countDistinct("unit_price").alias("unique_products"))
    .filter(F.col("unique_products") >= 3)
    .select("quantity_bucket", "unique_products")
)

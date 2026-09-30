from pyspark.sql import functions as F

valid_order_items = (
    df_order_items
    .filter(F.col("order_id").isin([1, 5, 10]) & (F.col("quantity") != 3))
    .withColumn("quantity_adjusted", F.round((F.col("quantity") * 1.1), 2))
)

order_items_stats = (
    valid_order_items
    .groupBy(
        F.when(F.col("quantity") >= 8, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.countDistinct("unit_price").alias("unique_products"),
        F.min("quantity").alias("min_quantity"),
    )
)

result = (
    order_items_stats
    .filter(F.col("min_quantity") >= 4.8)
    .select("quantity_bucket", "unique_products", "min_quantity")
    .orderBy("unique_products")
)

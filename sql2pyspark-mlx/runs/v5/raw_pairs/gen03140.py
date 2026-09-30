from pyspark.sql import functions as F

filtered_order_items = df_order_items.filter(F.col("product_id").isNotNull() & (F.col("product_id") == 36))

summary = (
    filtered_order_items
    .groupBy(
        F.when(F.col("quantity") >= 7, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.min("unit_price").alias("min_unit_price"),
    )
)

result = summary.select("quantity_bucket", "max_quantity", "min_unit_price")

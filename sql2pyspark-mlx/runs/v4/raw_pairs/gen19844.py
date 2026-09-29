from pyspark.sql import functions as F

filtered_order_items = df_order_items.filter(F.col("order_id").isin([1, 23, 31]))

summary = (
    filtered_order_items
    .groupBy("product_id")
    .agg(
        F.min("quantity").alias("min_quantity"),
        F.avg("unit_price").alias("avg_unit_price"),
    )
)

result = (
    summary
    .filter(F.col("min_quantity") > 6.1)
    .select("product_id", "min_quantity", "avg_unit_price")
    .orderBy("avg_unit_price")
)

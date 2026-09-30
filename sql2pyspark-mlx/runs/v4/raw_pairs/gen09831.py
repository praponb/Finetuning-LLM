from pyspark.sql import functions as F

filtered_order_items = df_order_items.filter(F.col("quantity") <= 5)

order_id_summary = (
    filtered_order_items
    .groupBy("order_id", "product_id")
    .agg(
        F.min("quantity").alias("min_quantity"),
        F.avg("unit_price").alias("avg_unit_price"),
    )
)

result = (
    order_id_summary
    .filter(F.col("min_quantity") >= 7.6)
    .select("order_id", "product_id", "min_quantity")
)

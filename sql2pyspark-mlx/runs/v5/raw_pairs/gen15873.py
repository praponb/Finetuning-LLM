from pyspark.sql import functions as F

order_items_by_product = (
    df_order_items
    .groupBy("product_id")
    .agg(F.min("unit_price").alias("min_unit_price"))
)

result = (
    df_products.alias("p")
    .join(order_items_by_product.alias("t"), F.col("p.id") == F.col("t.product_id"), "left")
    .select(F.col("p.launch_date"), F.col("t.min_unit_price"))
    .orderBy("launch_date")
)

from pyspark.sql import functions as F

order_items_by_product = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.max("unit_price").alias("max_unit_price"),
        F.avg("quantity").alias("avg_quantity"),
    )
)

result = (
    df_products.alias("p")
    .join(order_items_by_product.alias("t"), F.col("p.id") == F.col("t.product_id"), "inner")
    .filter(F.col("t.avg_quantity") > 3.1)
    .select(F.col("p.price"), F.col("t.max_unit_price"), F.col("t.avg_quantity"))
    .orderBy(F.col("max_unit_price").desc())
)

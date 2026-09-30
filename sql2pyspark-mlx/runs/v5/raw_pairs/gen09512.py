from pyspark.sql import functions as F

order_items_by_product = (
    df_order_items
    .groupBy("product_id")
    .agg(F.round(F.avg("unit_price"), 2).alias("avg_unit_price"))
)

result = (
    df_products.alias("p")
    .join(order_items_by_product.alias("O"), F.col("p.id") == F.col("O.product_id"), "left")
    .filter(F.col("O.avg_unit_price") >= 58)
    .select(
        F.col("p.id"),
        F.col("p.supplier_id"),
        F.col("p.product_name"),
        F.col("O.avg_unit_price"),
    )
    .orderBy(F.col("product_name").desc())
)

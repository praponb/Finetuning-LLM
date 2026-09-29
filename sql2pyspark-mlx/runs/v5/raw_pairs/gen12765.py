from pyspark.sql import functions as F

order_items_by_product = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"),
        F.avg("unit_price").alias("avg_unit_price"),
    )
)

result = (
    df_products.alias("p")
    .join(order_items_by_product.alias("oibp"), F.col("p.id") == F.col("oibp.product_id"), "left")
    .select(
        F.col("p.id"),
        F.col("p.product_name"),
        F.col("oibp.revenue"),
        F.col("oibp.avg_unit_price"),
    )
    .orderBy("id", F.col("revenue").desc())
)

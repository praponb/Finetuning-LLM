from pyspark.sql import functions as F

product_summary = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.count("quantity").alias("quantity_count"),
        F.min("unit_price").alias("min_unit_price"),
    )
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .select(
        F.col("p.product_name"),
        F.col("p.supplier_id"),
        F.col("ps.quantity_count"),
        F.col("ps.min_unit_price"),
    )
    .orderBy("product_name")
)

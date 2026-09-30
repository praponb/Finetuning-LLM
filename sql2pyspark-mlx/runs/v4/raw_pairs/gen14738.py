from pyspark.sql import functions as F

product_totals = (
    df_order_items
    .groupBy("product_id")
    .agg(F.count("unit_price").alias("unit_price_count"))
)

result = (
    df_products.alias("products")
    .join(product_totals.alias("t"), F.col("products.id") == F.col("t.product_id"), "inner")
    .select(
        F.col("products.id"),
        F.col("products.supplier_id"),
        F.col("products.category"),
        F.col("t.unit_price_count"),
    )
)

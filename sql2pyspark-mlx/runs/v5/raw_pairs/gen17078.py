from pyspark.sql import functions as F

product_summary = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .groupBy("product_id")
    .agg((F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"))
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .select(F.col("p.brand"), F.col("ps.unit_price_range"))
)

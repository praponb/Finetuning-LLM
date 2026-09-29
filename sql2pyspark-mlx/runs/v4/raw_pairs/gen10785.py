from pyspark.sql import functions as F

product_summary = (
    df_order_items
    .filter(F.col("product_id").isNull())
    .groupBy("product_id")
    .agg(
        F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
    )
)

result = (
    df_products.alias("products")
    .join(product_summary.alias("product_summary"), F.col("products.id") == F.col("product_summary.product_id"), "inner")
    .select(
        F.col("products.id"),
        F.col("products.product_name"),
        F.col("product_summary.revenue"),
        F.col("product_summary.avg_quantity"),
    )
)

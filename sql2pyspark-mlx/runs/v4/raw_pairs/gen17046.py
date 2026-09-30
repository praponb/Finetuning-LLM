from pyspark.sql import functions as F

product_stats = (
    df_order_items
    .filter(F.col("unit_price") <= 230)
    .groupBy("product_id")
    .agg(
        F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
    )
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .select(
        F.col("p.id"),
        F.col("p.product_name"),
        F.col("ps.revenue"),
        F.col("ps.avg_unit_price"),
    )
    .orderBy("product_name")
)

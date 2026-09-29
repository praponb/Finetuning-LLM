from pyspark.sql import functions as F

product_stats = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.countDistinct("unit_price").alias("unique_products"),
        F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"),
    )
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .filter(F.col("ps.unique_products") >= 4)
    .select(
        F.col("p.id"),
        F.col("p.launch_date"),
        F.col("ps.unique_products"),
        F.col("ps.revenue"),
    )
    .orderBy(F.col("unique_products").desc(), "id")
    .limit(5)
)

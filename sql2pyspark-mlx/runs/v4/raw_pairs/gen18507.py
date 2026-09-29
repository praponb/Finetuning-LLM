from pyspark.sql import functions as F

product_summary = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.count("*").alias("total_count"),
        F.countDistinct("unit_price").alias("unique_products"),
    )
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .select(F.col("p.cost"), F.col("ps.total_count"), F.col("ps.unique_products"))
    .orderBy(F.col("total_count").desc(), F.col("unique_products").desc())
)

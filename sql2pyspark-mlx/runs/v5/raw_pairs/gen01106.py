from pyspark.sql import functions as F

product_stats = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        F.min("sale_date").alias("min_sale_date"),
        F.avg("revenue").alias("avg_revenue"),
    )
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "left")
    .filter(F.col("ps.avg_revenue") >= 4000)
    .select(
        F.col("p.id"),
        F.col("p.category"),
        F.col("p.brand"),
        F.col("ps.min_sale_date"),
        F.col("ps.avg_revenue"),
    )
    .orderBy("id")
)

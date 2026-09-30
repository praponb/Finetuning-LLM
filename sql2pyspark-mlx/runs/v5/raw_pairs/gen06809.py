from pyspark.sql import functions as F

product_stats = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        F.max("sale_date").alias("last_sale_date"),
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
    )
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "left")
    .select(
        F.col("p.launch_date"),
        F.col("p.category"),
        F.col("ps.last_sale_date"),
        F.col("ps.avg_revenue"),
    )
)

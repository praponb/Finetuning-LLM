from pyspark.sql import functions as F

product_totals = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
        F.count("revenue").alias("revenue_count"),
    )
)

result = (
    df_products.alias("products")
    .join(product_totals.alias("product_totals"), F.col("products.id") == F.col("product_totals.product_id"), "left")
    .select(
        F.col("products.price"),
        F.col("products.brand"),
        F.col("product_totals.avg_revenue"),
        F.col("product_totals.revenue_count"),
    )
    .orderBy("brand")
)

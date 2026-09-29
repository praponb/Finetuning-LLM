from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.alias("ss"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .groupBy(F.col("p.category"))
    .agg(
        F.count(F.col("ss.id")).alias("store_sale_count"),
        F.min(F.col("ss.revenue")).alias("min_revenue"),
    )
)

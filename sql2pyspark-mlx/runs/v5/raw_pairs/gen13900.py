from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .groupBy(F.col("p.category"))
    .agg(
        F.max(F.col("ss.revenue")).alias("max_revenue"),
        F.avg(F.col("ss.revenue")).alias("avg_revenue"),
    )
    .orderBy("max_revenue")
)

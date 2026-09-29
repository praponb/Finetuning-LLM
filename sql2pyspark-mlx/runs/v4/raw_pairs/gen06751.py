from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "inner")
    .join(df_stores.alias("stores"), F.col("store_sales.store_id") == F.col("stores.id"), "right")
    .filter(F.col("store_sales.store_id") == 2)
    .groupBy(
        F.date_format(F.col("products.launch_date"), "yyyy-MM").alias("launch_month"),
    )
    .agg(
        F.max(F.col("store_sales.revenue")).alias("max_revenue"),
        F.round(F.avg(F.col("store_sales.revenue")), 2).alias("avg_revenue"),
    )
    .filter(F.col("avg_revenue") > 45)
)

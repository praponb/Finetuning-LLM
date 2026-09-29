from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "inner")
    .join(df_stores.alias("stores"), F.col("store_sales.store_id") == F.col("stores.id"), "inner")
    .filter(F.col("products.launch_date") >= "2025-06-01")
    .groupBy(F.col("products.category"))
    .agg(
        F.min(F.col("store_sales.sale_date")).alias("min_sale_date"),
        F.max(F.col("store_sales.sale_date")).alias("latest_sale_date"),
    )
)

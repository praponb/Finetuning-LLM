from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "right")
    .join(df_stores.alias("stores"), F.col("store_sales.store_id") == F.col("stores.id"), "inner")
    .groupBy(F.col("products.brand"))
    .agg(F.sum(F.col("store_sales.revenue")).alias("total_revenue"))
    .orderBy(F.col("total_revenue").desc(), "brand")
)

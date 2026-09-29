from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("store_sales.store_id"), "inner")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "left")
    .filter(F.col("store_sales.product_id").isin([2, 8]))
    .select(
        F.col("store_sales.id"),
        F.col("store_sales.product_id"),
        F.col("stores.opened_date"),
        F.col("stores.region"),
        F.col("products.product_name"),
    )
)

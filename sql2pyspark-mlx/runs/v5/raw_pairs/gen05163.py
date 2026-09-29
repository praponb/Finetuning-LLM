from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("products.id") == F.col("store_sales.product_id"), "inner")
    .join(df_stores.alias("stores"), F.col("store_sales.store_id") == F.col("stores.id"), "inner")
    .filter(F.col("store_sales.revenue") > 3400)
    .select(
        F.col("store_sales.id"),
        F.col("store_sales.store_id"),
        F.col("store_sales.sale_date"),
        F.col("products.category"),
        F.col("stores.sqft"),
    )
)

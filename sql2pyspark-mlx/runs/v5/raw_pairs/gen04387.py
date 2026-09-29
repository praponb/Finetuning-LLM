from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "inner")
    .join(df_stores.alias("stores"), F.col("store_sales.store_id") == F.col("stores.id"), "inner")
    .select(
        F.col("store_sales.sale_date"),
        F.col("products.brand"),
        F.col("products.category"),
        F.col("stores.opened_date"),
        F.col("stores.store_name"),
    )
    .orderBy(F.col("store_name").desc())
)

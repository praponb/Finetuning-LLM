from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "right")
    .join(df_stores.alias("stores"), F.col("store_sales.store_id") == F.col("stores.id"), "left")
    .select(
        F.col("store_sales.id"),
        F.col("store_sales.product_id"),
        F.col("store_sales.revenue"),
        F.col("products.brand"),
        F.col("products.supplier_id"),
        F.col("stores.region"),
        F.col("stores.sqft"),
    )
    .orderBy(F.col("region").desc())
)

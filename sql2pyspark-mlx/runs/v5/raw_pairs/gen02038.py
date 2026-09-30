from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("store_sales.store_id"), "outer")
    .join(df_products.alias("products"), F.col("products.id") == F.col("store_sales.product_id"), "left")
    .select(
        F.col("store_sales.id"),
        F.col("store_sales.revenue"),
        F.col("stores.opened_date"),
        F.col("stores.sqft"),
        F.col("products.category"),
    )
)

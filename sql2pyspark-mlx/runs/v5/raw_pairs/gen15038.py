from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("store_sales.store_id"), "left")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "inner")
    .select(
        F.col("store_sales.id"),
        F.col("store_sales.revenue"),
        F.col("stores.sqft"),
        F.col("stores.region"),
        F.col("products.launch_date"),
    )
    .orderBy("region")
)

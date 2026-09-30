from pyspark.sql import functions as F

result = (
    df_stores.alias("stores")
    .join(df_store_sales.alias("store_sales"), F.col("stores.id") == F.col("store_sales.store_id"), "inner")
    .select(
        F.col("stores.region"),
        F.col("store_sales.store_id"),
        F.col("store_sales.product_id"),
        F.round((F.col("stores.sqft") * 1.15), 2).alias("sqft_scaled"),
    )
)

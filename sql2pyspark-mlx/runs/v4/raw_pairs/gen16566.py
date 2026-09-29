from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "inner")
    .select(
        F.col("store_sales.id"),
        F.col("store_sales.store_id"),
        F.col("products.stock_qty"),
        F.col("products.category"),
        F.col("products.launch_date"),
    )
)

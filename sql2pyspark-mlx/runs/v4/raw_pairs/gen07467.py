from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "inner")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("store_sales.store_id"), "inner")
    .select(
        F.col("store_sales.revenue"),
        F.col("store_sales.sale_date"),
        F.col("products.stock_qty"),
        F.col("stores.opened_date"),
        F.floor(F.col("products.price")).alias("price_whole"),
    )
    .orderBy("opened_date", F.col("price_whole").desc())
)

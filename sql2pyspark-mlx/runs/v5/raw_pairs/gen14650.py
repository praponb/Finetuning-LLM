from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "inner")
    .join(df_stores.alias("stores"), F.col("store_sales.store_id") == F.col("stores.id"), "inner")
    .filter((F.col("stores.region") == "North") & F.col("store_sales.product_id").isin([5, 6]))
    .select(
        F.col("store_sales.id"),
        F.col("store_sales.revenue"),
        F.col("products.brand"),
        F.col("stores.opened_date"),
    )
    .orderBy("id")
    .limit(10)
)

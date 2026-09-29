from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_stores.alias("stores"), F.col("store_sales.store_id") == F.col("stores.id"), "left")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "outer")
    .filter(F.col("store_sales.product_id").isin([1, 2, 10]))
    .select(
        F.col("store_sales.sale_date"),
        F.col("store_sales.store_id"),
        F.col("stores.city"),
        F.col("products.brand"),
        F.col("products.cost"),
    )
    .orderBy("sale_date")
)

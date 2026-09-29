from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_store_sales.alias("store_sales"), F.col("products.id") == F.col("store_sales.product_id"), "left")
    .filter(F.array_contains(F.col("products.tags"), "premium"))
    .select(
        F.col("products.brand"),
        F.col("products.product_name"),
        F.col("products.supplier_id"),
        F.col("store_sales.revenue"),
        F.col("store_sales.product_id"),
    )
)

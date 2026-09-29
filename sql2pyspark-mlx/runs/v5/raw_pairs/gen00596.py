from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "left")
    .filter((F.col("products.price") > 730) | F.array_contains(F.col("products.tags"), "new"))
    .select(
        F.col("store_sales.id"),
        F.col("store_sales.revenue"),
        F.col("store_sales.sale_date"),
        F.col("store_sales.product_id"),
        F.col("products.cost"),
        F.col("products.product_name"),
        F.col("products.category"),
    )
)

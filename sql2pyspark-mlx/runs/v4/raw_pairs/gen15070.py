from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_store_sales.alias("store_sales"), F.col("store_sales.product_id") == F.col("products.id"), "left")
    .select(
        F.col("products.cost"),
        F.col("products.price"),
        F.col("store_sales.sale_date"),
        F.col("store_sales.revenue"),
        F.col("store_sales.product_id"),
    )
    .orderBy(F.col("price").desc())
)

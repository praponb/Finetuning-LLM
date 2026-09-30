from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_store_sales.filter(F.col("revenue").between(2500, 3300)).alias("store_sales"), F.col("products.id") == F.col("store_sales.product_id"), "left_anti")
    .select(F.col("products.id"), F.col("products.category"))
)

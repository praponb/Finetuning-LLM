from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.upper("product_name").like("%APT%"))
    .select("price")
    .intersect(df_store_sales.filter(F.col("product_id") == 10).select("revenue"))
)

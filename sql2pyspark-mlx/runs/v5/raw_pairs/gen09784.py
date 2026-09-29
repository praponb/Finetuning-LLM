from pyspark.sql import functions as F

result = (
    df_products
    .select("product_name", "price", "stock_qty")
    .orderBy(F.col("product_name").desc())
)

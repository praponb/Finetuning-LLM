from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").like("U%"))
    .select("id", "price")
    .orderBy(F.col("price").desc())
)

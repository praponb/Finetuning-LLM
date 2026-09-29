from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.size("tags") >= 2) | (F.col("price") >= 750))
    .select("id", "category", "brand", "price")
)

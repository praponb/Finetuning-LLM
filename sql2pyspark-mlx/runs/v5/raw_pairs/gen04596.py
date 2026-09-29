from pyspark.sql import functions as F

result = (
    df_products
    .select("id", "brand", F.length("product_name").alias("product_name_length"))
    .orderBy(F.col("id").desc())
)

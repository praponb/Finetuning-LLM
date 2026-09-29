from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost").between(290, 490))
    .select("id", "product_name", "category", F.size("tags").alias("tags_count"))
)

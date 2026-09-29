from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category") != "Books")
    .select("id", "category", F.explode("tags").alias("tag"))
)

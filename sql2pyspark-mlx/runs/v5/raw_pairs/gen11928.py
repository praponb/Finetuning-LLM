from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category").isin(["Books", "Sports", "Home"]))
    .select("id", "brand", F.explode("tags").alias("tag"))
)

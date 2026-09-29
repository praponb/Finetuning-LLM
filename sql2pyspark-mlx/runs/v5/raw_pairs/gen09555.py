from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category").isin(["Sports", "Home", "Grocery"]))
    .select("id", "brand", "category", F.explode("tags").alias("tag_name"))
)

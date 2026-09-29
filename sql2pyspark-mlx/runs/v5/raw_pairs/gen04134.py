from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category") == "Home")
    .select("id", "price", "brand", F.explode("tags").alias("tag_name"))
)

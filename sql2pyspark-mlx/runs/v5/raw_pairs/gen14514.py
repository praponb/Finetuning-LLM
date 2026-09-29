from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category") == "Fashion")
    .select("id", "price", "product_name", F.explode("tags").alias("tag_name"))
)

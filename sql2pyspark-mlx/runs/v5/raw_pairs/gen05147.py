from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price") < 1120)
    .select("id", "category", F.explode("tags").alias("tag_name"))
)

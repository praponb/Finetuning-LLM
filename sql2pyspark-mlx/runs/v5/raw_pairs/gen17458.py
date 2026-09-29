from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand") == "Initech")
    .select("id", "category", F.explode("tags").alias("tag_name"))
)

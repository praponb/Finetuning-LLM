from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price").between(1110, 1120))
    .select("id", "price", F.explode("tags").alias("tag_name"))
)

from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price") > 710)
    .select("id", "price", F.explode("tags").alias("label"))
)

from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost") <= 650)
    .select("id", "brand", F.explode("tags").alias("label"))
)

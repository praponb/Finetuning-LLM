from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost") >= 240)
    .select("id", "brand", F.explode("tags").alias("tag"))
)

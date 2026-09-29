from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand") == "Globex")
    .select("id", "price", "brand", F.explode("tags").alias("label"))
)

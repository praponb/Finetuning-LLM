from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost").between(270, 360))
    .select("id", "category", "brand", F.explode("tags").alias("label"))
)

from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id") == 34)
    .select("id", "brand", F.explode("tags").alias("label"))
)

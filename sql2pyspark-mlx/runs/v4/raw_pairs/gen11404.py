from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isin([16, 33]))
    .select("id", "brand", "price", F.explode("tags").alias("tag_name"))
)

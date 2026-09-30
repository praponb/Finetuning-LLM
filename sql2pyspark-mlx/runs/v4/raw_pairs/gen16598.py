from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isin([9, 10, 21]))
    .select("id", "category", "price", F.explode("tags").alias("tag"))
)

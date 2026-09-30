from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.array_contains("tags", "eco"))
    .select("id", "price", F.explode("tags").alias("tag_name"))
)

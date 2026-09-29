from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.array_contains("tags", "sale"))
    .select("id", "brand", "category", F.explode("tags").alias("tag_name"))
)

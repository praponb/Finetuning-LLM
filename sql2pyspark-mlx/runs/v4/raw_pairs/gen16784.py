from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.array_contains("tags", "clearance"))
    .select("id", "product_name", "brand", F.explode("tags").alias("tag_name"))
)

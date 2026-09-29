from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.array_contains("tags", "eco"))
    .withColumn("brand_lower", F.lower("brand"))
    .withColumn("price_rounded", F.round("price", 1))
)

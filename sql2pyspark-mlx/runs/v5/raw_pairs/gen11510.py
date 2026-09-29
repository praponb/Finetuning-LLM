from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.array_contains("tags", "bestseller") & (F.col("brand") != "Umbrella"))
    .groupBy("category", "brand")
    .agg(F.count("*").alias("product_count"))
    .orderBy(F.col("product_count").desc())
)

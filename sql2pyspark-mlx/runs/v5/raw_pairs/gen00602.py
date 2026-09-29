from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("brand") == "Acme") & (F.col("cost") < 300) & (F.year("launch_date") == 2024))
    .select("id", "brand", "tags", F.size("tags").alias("tags_count"))
    .orderBy(F.col("id").desc())
)

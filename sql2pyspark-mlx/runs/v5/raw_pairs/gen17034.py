from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("price") < 420) & (F.col("cost") >= 470))
    .select("id", "tags", F.size("tags").alias("tags_count"))
)

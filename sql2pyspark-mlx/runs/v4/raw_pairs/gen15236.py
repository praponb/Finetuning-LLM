from pyspark.sql import functions as F

result = (
    df_stores
    .select("id", "sqft", F.trim("store_name").alias("clean_store_name"))
    .orderBy(F.col("clean_store_name").desc(), F.col("id").desc())
)

from pyspark.sql import functions as F

result = (
    df_stores
    .select("id", "city", "region", F.lower("region").alias("region_lower"))
    .orderBy("id")
)

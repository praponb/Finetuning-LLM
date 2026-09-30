from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").isNotNull())
    .select("region")
    .distinct()
    .orderBy(F.col("region").desc())
)

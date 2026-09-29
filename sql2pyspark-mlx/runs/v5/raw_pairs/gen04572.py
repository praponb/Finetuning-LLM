from pyspark.sql import functions as F

result = (
    df_stores
    .select("region")
    .distinct()
    .orderBy(F.col("region").desc())
    .limit(20)
)

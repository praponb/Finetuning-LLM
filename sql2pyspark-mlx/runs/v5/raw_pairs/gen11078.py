from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isNull())
    .select("city", "region", "sqft", F.round("sqft", 2).alias("sqft_rounded"))
    .orderBy(F.col("sqft").desc())
)

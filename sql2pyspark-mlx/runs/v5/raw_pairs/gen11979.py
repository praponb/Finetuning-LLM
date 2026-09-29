from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("city")
    .agg(
        F.sum(F.when(F.col("city") == "Chiang Mai", F.col("rating")).otherwise(0)).alias("chiang_mai_rating"),
        F.min("rating").alias("min_rating"),
    )
    .orderBy(F.col("chiang_mai_rating").desc())
)

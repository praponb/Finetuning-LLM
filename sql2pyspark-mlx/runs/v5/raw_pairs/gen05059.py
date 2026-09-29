from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region", "city")
    .agg(
        F.sum(F.when(F.col("region") == "Central", 1).otherwise(0)).alias("central_count"),
    )
    .orderBy(F.col("central_count").desc())
)

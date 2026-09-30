from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .agg(
        F.sum(F.when(F.col("region") == "North", 1).otherwise(0)).alias("north_count"),
        F.count("*").alias("total_count"),
    )
    .filter(F.col("north_count") < 67)
    .orderBy("total_count", F.col("city").desc())
)

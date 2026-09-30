from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("city")
    .agg(
        F.avg("rating").alias("avg_rating"),
        F.sum(F.when(F.col("city") == "London", 1).otherwise(0)).alias("london_count"),
    )
    .filter(F.col("london_count") < 62)
    .select("city", "avg_rating", "london_count")
)

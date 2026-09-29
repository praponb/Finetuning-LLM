from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country") == "SG")
    .groupBy(
        F.when(F.col("rating") >= 4.2, "high").when(F.col("rating") >= 2.0, "mid").otherwise("low").alias("rating_bucket"),
        "country",
    )
    .agg(
        F.sum(F.when(F.col("city") == "Berlin", F.col("rating")).otherwise(0)).alias("berlin_rating"),
        F.sum(F.when(F.col("city") == "London", F.col("rating")).otherwise(0)).alias("london_rating"),
    )
    .orderBy("berlin_rating")
)

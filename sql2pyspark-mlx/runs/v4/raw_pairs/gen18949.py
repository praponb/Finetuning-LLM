from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country") == "SG")
    .groupBy(
        "country",
        F.when(F.col("rating") >= 3.8, "high").when(F.col("rating") >= 1.7, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("country") == "DE", F.col("rating")).otherwise(0)).alias("de_rating"),
        F.avg("rating").alias("avg_rating"),
        F.round(((100 * F.sum(F.when(F.col("city") == "London", 1).otherwise(0))) / F.count("*")), 1).alias("pct_london"),
    )
    .filter(F.col("pct_london") >= 43)
)

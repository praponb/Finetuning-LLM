from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city") != "Bangkok")
    .groupBy(
        "city",
        "country",
        F.when(F.col("rating") >= 4.5, "high").when(F.col("rating") >= 1.7, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("city") == "Bangkok", 1).otherwise(0)).alias("bangkok_count"),
        F.avg("rating").alias("avg_rating"),
    )
    .orderBy(F.col("bangkok_count").desc(), "city", "country", "rating_bucket")
    .limit(1)
)

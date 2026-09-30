from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country").isin(["US", "TH"]) & (F.col("rating") <= 3.3))
    .groupBy(
        F.when(F.col("rating") >= 4.4, "high").when(F.col("rating") >= 1.6, "mid").otherwise("low").alias("rating_bucket"),
        "country",
    )
    .agg(
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.count(F.when(F.col("country") == "UK", 1)).alias("uk_count"),
    )
    .orderBy("uk_count", "rating_bucket", "country")
    .limit(1)
)

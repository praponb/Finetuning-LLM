from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.3, "high").when(F.col("rating") >= 2.4, "mid").otherwise("low").alias("rating_bucket"),
        "country",
    )
    .agg(F.round(F.avg("rating"), 2).alias("avg_rating"))
    .filter(F.col("avg_rating") >= 26)
    .orderBy(F.col("country").desc())
)

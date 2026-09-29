from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isin(["Chiang Mai", "Berlin"]))
    .groupBy(
        F.when(F.col("rating") >= 4.2, "high").when(F.col("rating") >= 1.6, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.round(F.avg("rating"), 2).alias("avg_rating"))
    .orderBy("rating_bucket")
)

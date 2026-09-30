from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating") <= 4.1) | (F.col("rating") < 3.5))
    .groupBy(
        F.when(F.col("rating") >= 3.7, "high").when(F.col("rating") >= 1.5, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.avg("rating").alias("avg_rating"))
)

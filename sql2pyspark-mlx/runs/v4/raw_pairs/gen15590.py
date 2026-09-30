from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.3, "high").when(F.col("rating") >= 2.2, "mid").otherwise("low").alias("rating_bucket"),
        "city",
    )
    .agg(F.round(F.avg("rating"), 2).alias("avg_rating"))
    .orderBy("city")
)

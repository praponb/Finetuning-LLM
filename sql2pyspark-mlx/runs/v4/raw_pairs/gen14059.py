from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") >= 2.9)
    .cube(
        F.when(F.col("rating") >= 3.8, "high").when(F.col("rating") >= 1.9, "mid").otherwise("low").alias("rating_bucket"),
        "city",
    )
    .agg(F.avg("rating").alias("avg_rating"))
    .orderBy("rating_bucket")
)

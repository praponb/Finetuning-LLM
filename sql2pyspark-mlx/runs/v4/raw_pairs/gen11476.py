from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("country") == "TH") & (F.col("rating") >= 3.0))
    .groupBy(
        F.when(F.col("rating") >= 4.5, "high").when(F.col("rating") >= 2.2, "mid").otherwise("low").alias("rating_bucket"),
        "country",
    )
    .agg(F.min("rating").alias("min_rating"))
    .orderBy(F.col("country").desc())
)

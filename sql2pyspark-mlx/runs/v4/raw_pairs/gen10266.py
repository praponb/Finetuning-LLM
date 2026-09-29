from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating") > 3.0) & F.col("country").isin(["US", "TH"]))
    .groupBy(
        F.when(F.col("rating") >= 3.6, "high").when(F.col("rating") >= 2.0, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.min("rating").alias("min_rating"))
    .orderBy("rating_bucket")
)

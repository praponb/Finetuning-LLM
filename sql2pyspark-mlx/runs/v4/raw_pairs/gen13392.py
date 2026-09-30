from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country") != "UK")
    .groupBy(F.when(F.col("rating") >= 4.5, "high").when(F.col("rating") >= 1.5, "mid").otherwise("low").alias("rating_bucket"))
    .pivot("country", ["DE", "SG", "US"])
    .agg(F.count("rating"))
    .orderBy("rating_bucket")
)

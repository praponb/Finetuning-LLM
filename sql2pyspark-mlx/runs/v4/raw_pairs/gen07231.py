from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(F.when(F.col("rating") >= 3.8, "high").when(F.col("rating") >= 2.1, "mid").otherwise("low").alias("rating_bucket"))
    .pivot("country", ["UK", "DE", "US"])
    .agg(F.avg("rating"))
)

from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(F.when(F.col("rating") >= 4.1, "high").when(F.col("rating") >= 2.0, "mid").otherwise("low").alias("rating_bucket"))
    .pivot("city", ["Tokyo", "Bangkok", "Berlin"])
    .agg(F.max("rating"))
)

from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(F.when(F.col("rating") >= 3.5, "high").when(F.col("rating") >= 2.0, "mid").otherwise("low").alias("rating_bucket"))
    .pivot("city", ["Tokyo", "Berlin", "Bangkok", "Chiang Mai"])
    .agg(F.count("rating"))
)

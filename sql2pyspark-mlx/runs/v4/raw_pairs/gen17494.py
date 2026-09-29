from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        "city",
        F.when(F.col("rating") >= 3.5, "high").when(F.col("rating") >= 2.4, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        (F.max("rating") - F.min("rating")).alias("rating_range"),
        F.sum(F.when(F.col("country") == "SG", 1).otherwise(0)).alias("sg_count"),
    )
)

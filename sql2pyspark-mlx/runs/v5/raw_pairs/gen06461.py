from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country").isin(["TH", "UK"]))
    .groupBy(
        F.when(F.col("rating") >= 4.4, "high").when(F.col("rating") >= 2.6, "mid").otherwise("low").alias("rating_bucket"),
        "city",
    )
    .agg(
        F.count("rating").alias("rating_count"),
        F.sum(F.when(F.col("city") == "London", 1).otherwise(0)).alias("london_count"),
    )
    .orderBy("london_count")
)

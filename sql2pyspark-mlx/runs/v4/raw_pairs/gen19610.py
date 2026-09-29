from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("country") == "UK") & F.col("country").isin(["JP", "US", "UK"]))
    .groupBy(
        "city",
        F.when(F.col("rating") >= 3.6, "high").when(F.col("rating") >= 1.4, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        (F.max("rating") - F.min("rating")).alias("rating_range"),
        F.sum("rating").alias("total_rating"),
    )
    .orderBy(F.col("total_rating").desc())
)

from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.lower("supplier_name").like("%iam%") & (F.col("supplier_name").isNotNull() | (F.col("country") == "US")))
    .groupBy(
        "city",
        F.when(F.col("rating") >= 3.5, "high").when(F.col("rating") >= 2.1, "mid").otherwise("low").alias("rating_bucket"),
        "country",
    )
    .agg(
        F.sum(F.when(F.col("country") == "JP", F.col("rating")).otherwise(0)).alias("jp_rating"),
        (F.max("rating") - F.min("rating")).alias("rating_range"),
        F.sum("rating").alias("total_rating"),
    )
    .filter(F.col("rating_range") >= 69)
    .orderBy("country", "rating_range")
)

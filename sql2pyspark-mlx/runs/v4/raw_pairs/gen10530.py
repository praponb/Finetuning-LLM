from pyspark.sql import functions as F

valid_suppliers = (
    df_suppliers
    .filter(F.col("country").isin(["JP", "TH", "DE"]) | (F.length("supplier_name") >= 14))
)

summary = (
    valid_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.1, "high").when(F.col("rating") >= 1.6, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.count("rating").alias("rating_count"),
        F.sum("rating").alias("total_rating"),
    )
)

result = (
    summary
    .filter(F.col("rating_count") < 3)
    .select("rating_bucket", "rating_count", "total_rating")
)

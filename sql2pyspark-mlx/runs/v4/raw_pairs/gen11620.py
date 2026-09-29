from pyspark.sql import functions as F

base = (
    df_suppliers
    .filter((F.col("country") == "US") & ((F.col("country") == "US") | F.col("supplier_name").like("Gl%")))
)

rating_bucket_summary = (
    base
    .groupBy(
        F.when(F.col("rating") >= 4.2, "high").when(F.col("rating") >= 1.6, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("city") == "Berlin", 1).otherwise(0))) / F.count("*")), 1).alias("pct_berlin"),
    )
)

result = (
    rating_bucket_summary
    .filter(F.col("pct_berlin") < 40)
    .select("rating_bucket", "pct_berlin")
)

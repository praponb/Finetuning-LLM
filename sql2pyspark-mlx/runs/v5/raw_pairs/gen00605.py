from pyspark.sql import functions as F

base = (
    df_suppliers
    .filter(F.col("supplier_name").like("G%") & F.col("country").isin(["DE", "US"]) & ~F.col("country").isin(["JP", "DE", "TH"]))
)

rating_bucket_summary = (
    base
    .groupBy(
        F.when(F.col("rating") >= 3.5, "high").when(F.col("rating") >= 1.6, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        (F.max("rating") - F.min("rating")).alias("rating_range"),
        F.count(F.when(F.col("country") == "TH", 1)).alias("th_count"),
    )
)

result = (
    rating_bucket_summary
    .filter(F.col("th_count") >= 9)
    .orderBy(F.col("rating_bucket").desc())
)

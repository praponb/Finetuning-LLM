from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(~(F.col("supplier_name").like("%Who%") & ((F.col("city") != "Chiang Mai") | (F.col("city") == "Chiang Mai"))))
    .groupBy(
        "country",
        F.when(F.col("rating") >= 3.9, "high").when(F.col("rating") >= 1.6, "mid").otherwise("low").alias("rating_bucket"),
        "city",
    )
    .agg(
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.sum(F.when(F.col("country") == "US", F.col("rating")).otherwise(0)).alias("us_rating"),
    )
    .orderBy(F.col("rating_bucket").desc(), "country")
)

from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 3.7, "LARGE").otherwise("SMALL").alias("rating_tier"),
    )
    .agg(
        F.count(F.when(F.col("city") == "Singapore", 1)).alias("singapore_rows"),
        F.count(F.when(F.col("country") == "US", 1)).alias("us_count"),
        F.count(F.when(F.col("city") == "Chiang Mai", 1)).alias("chiang_mai_rows"),
        F.count(F.when(F.col("city") == "Berlin", 1)).alias("berlin_count"),
    )
    .orderBy(F.col("chiang_mai_rows").desc())
)

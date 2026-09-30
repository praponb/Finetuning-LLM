from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.1, "LARGE").otherwise("SMALL").alias("rating_tier"),
    )
    .agg(
        F.count(F.when(F.col("country") == "SG", 1)).alias("sg_count"),
        F.sum(F.when(F.col("city") == "Berlin", F.col("rating")).otherwise(0)).alias("berlin_rating"),
        F.count("*").alias("num_rows"),
        F.count_if(F.col("city") == "New York").alias("new_york_events"),
        F.count(F.when(F.col("country") == "DE", 1)).alias("de_count"),
        F.count(F.when(F.col("city") == "New York", 1)).alias("new_york_count"),
    )
    .orderBy(F.col("num_rows").desc())
)

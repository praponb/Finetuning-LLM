from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.3, "HIGH").otherwise("LOW").alias("rating_tier"),
    )
    .agg(
        F.min("rating").alias("min_rating"),
        F.sum(F.when(F.col("city") == "Bangkok", F.col("rating")).otherwise(0)).alias("bangkok_rating"),
    )
    .filter(F.col("min_rating") < 1000000)
    .orderBy(F.col("min_rating").asc_nulls_first())
)

from pyspark.sql import functions as F

result = (
    df_suppliers
    .withColumn("rating_tier", F.when(F.col("rating") >= 3.6, "LARGE").otherwise("SMALL"))
    .groupingSets([["city", "rating_tier"], ["city"]], "city", "rating_tier")
    .agg(
        F.sum(F.when(F.col("country") == "UK", F.col("rating")).otherwise(0)).alias("uk_rating"),
        F.round(F.avg("rating"), 2).alias("avg_rating"),
    )
    .orderBy(F.col("rating_tier").asc_nulls_last())
)

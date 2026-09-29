from pyspark.sql import functions as F

result = (
    df_suppliers
    .withColumn("rating_tier", F.when(F.col("rating") >= 4.3, "HIGH").otherwise("LOW"))
    .groupingSets([["country", "rating_tier"], ["country"]], "country", "rating_tier")
    .agg(
        F.stddev_samp("rating").alias("rating_stddev"),
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.round(F.sum("rating") / F.nullif(F.count(F.when(F.col("country") == "DE", 1)), F.lit(0)), 2).alias("rating_per_de"),
        F.max(F.when(F.col("city") == "London", F.col("rating"))).alias("london_max_rating"),
    )
    .orderBy(F.col("london_max_rating").desc_nulls_last())
)

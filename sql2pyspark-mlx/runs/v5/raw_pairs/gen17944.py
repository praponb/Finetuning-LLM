from pyspark.sql import functions as F

filtered = (
    df_suppliers
    .filter(F.col("country") == "US")
    .withColumn("rating_level", F.when(F.col("rating") >= 3.9, "High").when(F.col("rating") >= 2.6, "Medium").otherwise("Low"))
    .withColumn("resolved_city", F.coalesce("city", F.lit("DIRECT")))
)

result = (
    filtered
    .withColumn("rating_tier", F.when(F.col("rating") >= 4.6, "PREMIUM").otherwise("STANDARD"))
    .groupingSets([["country", "rating_tier"], ["country"]], "country", "rating_tier")
    .agg(
        F.sum(F.when(F.col("country") == "TH", F.col("rating")).otherwise(0)).alias("th_rating"),
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.stddev_samp("rating").alias("rating_stddev"),
    )
)

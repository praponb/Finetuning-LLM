from pyspark.sql import functions as F

result = (
    df_suppliers
    .withColumn("rating_tier", F.when(F.col("rating") >= 3.9, "LARGE").otherwise("SMALL"))
    .groupingSets([["city", "rating_tier"], ["city"], []], "city", "rating_tier")
    .agg(
        F.count_if(F.col("city") == "Singapore").alias("singapore_events"),
        F.stddev_samp("rating").alias("rating_stddev"),
        F.grouping("city").alias("grouping_level"),
    )
    .orderBy(F.col("rating_tier").desc())
)

from pyspark.sql import functions as F

filtered = (
    df_stores
    .filter(F.col("region").isNotNull() & (F.length("store_name") < 12))
    .withColumn("adjusted_sqft", F.round((F.col("sqft") * 100), 2))
    .withColumn("region_clean", F.coalesce("region", F.lit("OTHER")))
)

result = (
    filtered
    .withColumn("adjusted_sqft_tier", F.when(F.col("adjusted_sqft") >= 65, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["adjusted_sqft_tier", "region"], ["adjusted_sqft_tier"]],
        "adjusted_sqft_tier",
        "region",
    )
    .agg(
        F.count_if(F.col("city") == "Berlin").alias("berlin_events"),
        F.stddev_samp("sqft").alias("sqft_stddev"),
        F.countDistinct("id").alias("distinct_ids"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("sqft_stddev") > 0)
    .orderBy(F.col("adjusted_sqft_tier").desc_nulls_last())
)

from pyspark.sql import functions as F

filtered = (
    df_stores
    .filter((F.col("opened_date") > F.lit("2023-11-03").cast("date")) & F.col("region").isNotNull())
    .withColumn("city_title", F.initcap(F.trim("city")))
    .withColumn("region_filled", F.coalesce("region", F.lit("OTHER")))
)

result = (
    filtered
    .withColumn("sqft_tier", F.when(F.col("sqft") >= 13973, "LARGE").otherwise("SMALL"))
    .rollup("sqft_tier")
    .agg(
        F.stddev_samp("sqft").alias("sqft_stddev"),
        F.count(F.when(F.col("region") == "South", 1)).alias("south_rows"),
        F.max(F.when(F.col("city") == "Bangkok", F.col("sqft"))).alias("bangkok_max_sqft"),
        F.stddev_samp("sqft").alias("sqft_stddev_2"),
        F.sum(F.when(F.col("region") == "West", F.col("sqft")).otherwise(0)).alias("west_sqft"),
        F.grouping("sqft_tier").alias("grouping_level"),
    )
)

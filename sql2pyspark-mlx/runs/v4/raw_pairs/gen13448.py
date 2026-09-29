from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("city") == "Bangkok") & (F.col("sqft") < 6838))
    .groupBy(
        F.when(F.col("sqft") >= 16013, "high").when(F.col("sqft") >= 7869, "mid").otherwise("low").alias("sqft_bucket"),
        "region",
    )
    .agg(
        F.round(F.stddev("sqft"), 2).alias("stddev_sqft"),
        F.countDistinct("sqft").alias("unique_regions"),
        F.count(F.when(F.col("city") == "Chiang Mai", 1)).alias("chiang_mai_count"),
    )
    .orderBy(F.col("stddev_sqft").desc())
)

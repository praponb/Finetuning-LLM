from pyspark.sql import functions as F

result = (
    df_stores
    .withColumn("sqft_tier", F.when(F.col("sqft") >= 15057, "HIGH").otherwise("LOW"))
    .rollup("region", "sqft_tier")
    .agg(
        F.percentile_approx("sqft", 0.9).alias("sqft_p90"),
        F.stddev_samp("sqft").alias("sqft_stddev"),
        F.grouping("region").alias("gid"),
    )
    .orderBy(F.col("sqft_stddev").asc_nulls_last())
)

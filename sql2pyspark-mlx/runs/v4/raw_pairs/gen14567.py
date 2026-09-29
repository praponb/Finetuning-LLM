from pyspark.sql import functions as F

result = (
    df_customers
    .cube(
        "city",
        F.when(F.col("credit_limit") >= 15800, "HIGH").otherwise("LOW").alias("credit_limit_tier"),
    )
    .agg(
        F.count_if(F.col("city") == "Bangkok").alias("bangkok_events"),
        F.round(F.sum("age") / F.nullif(F.count(F.when(F.col("country") == "US", 1)), F.lit(0)), 2).alias("age_per_us"),
        F.percentile_approx("credit_limit", 0.9).alias("credit_limit_p90"),
        F.stddev_samp("credit_limit").alias("credit_limit_stddev"),
        F.size(F.collect_set("country")).alias("num_countries"),
        F.count(F.when(F.col("country") == "US", 1)).alias("us_count"),
    )
    .orderBy(F.col("city").desc())
)

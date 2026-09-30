from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.date_trunc("WEEK", "opened_date").alias("week_start"))
    .agg(
        F.count(F.when(F.col("city") == "Singapore", 1)).alias("singapore_count"),
        F.count(F.when(F.col("city") == "London", 1)).alias("london_rows"),
        F.percentile_approx("sqft", [0.25, 0.5, 0.75]).alias("sqft_quartiles"),
        F.stddev_samp("sqft").alias("sqft_stddev"),
    )
    .orderBy(F.col("sqft_stddev").asc_nulls_first())
)

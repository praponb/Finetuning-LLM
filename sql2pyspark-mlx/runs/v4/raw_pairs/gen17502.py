from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        F.when(F.col("sqft") >= 12694, "PREMIUM").otherwise("STANDARD").alias("sqft_tier"),
        F.date_trunc("week", "opened_date").alias("week_start"),
    )
    .agg(
        F.stddev_samp("sqft").alias("sqft_stddev"),
        F.percentile_approx("sqft", [0.25, 0.5, 0.75]).alias("sqft_quartiles"),
        F.count(F.when(F.col("region") == "Central", 1)).alias("central_count"),
    )
    .filter(F.col("sqft_stddev") > 0)
    .orderBy("week_start")
)

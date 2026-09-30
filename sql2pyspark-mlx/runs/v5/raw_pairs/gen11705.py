from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        "country",
        F.when(F.col("rating") >= 4.3, "PREMIUM").otherwise("STANDARD").alias("rating_tier"),
    )
    .agg(
        F.percentile_approx("rating", 0.5).alias("rating_p50"),
        F.count("*").alias("event_count"),
        F.round(F.sum("rating") / F.nullif(F.count(F.when(F.col("country") == "JP", 1)), F.lit(0)), 2).alias("rating_per_jp"),
        F.stddev_samp("rating").alias("rating_stddev"),
    )
    .orderBy(F.col("rating_tier").desc())
)

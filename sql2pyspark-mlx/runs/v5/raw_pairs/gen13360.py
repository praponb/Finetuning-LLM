from pyspark.sql import functions as F

valid = (
    df_promotions
    .filter(F.col("discount_pct") >= 0.29)
    .withColumn("channel_promo_code_key", F.concat_ws("-", "channel", "promo_code"))
)

result = (
    valid
    .withColumn("week_start", F.date_trunc("WEEK", "ends_on"))
    .withColumn("discount_pct_tier", F.when(F.col("discount_pct") >= 0.31, "LARGE").otherwise("SMALL"))
    .rollup("week_start", "discount_pct_tier")
    .agg(
        F.count(F.when(F.col("channel") == "web", 1)).alias("web_count"),
        F.stddev_samp("discount_pct").alias("discount_pct_stddev"),
        F.count("*").alias("cnt"),
        F.percentile_approx("discount_pct", [0.25, 0.5, 0.75]).alias("discount_pct_quartiles"),
        F.count(F.when(F.col("promo_code") == "BUNDLE", 1)).alias("bundle_rows"),
        F.sum(F.when(F.col("promo_code") == "WELCOME5", F.col("discount_pct")).otherwise(0)).alias("welcome5_discount_pct"),
        F.grouping("week_start").alias("gid"),
    )
    .orderBy(F.col("cnt").desc())
)

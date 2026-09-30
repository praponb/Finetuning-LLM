from pyspark.sql import functions as F

result = (
    df_promotions
    .cube(F.date_trunc("WEEK", "starts_on").alias("week_start"))
    .agg(
        F.stddev_samp("discount_pct").alias("discount_pct_stddev"),
        F.size(F.collect_set("channel")).alias("num_channels"),
        F.countDistinct("channel").alias("distinct_channels"),
        F.percentile_approx("discount_pct", [0.25, 0.5, 0.75]).alias("discount_pct_quartiles"),
        F.stddev_samp("discount_pct").alias("discount_pct_stddev_2"),
        F.size(F.collect_set("promo_code")).alias("num_promo_codes"),
    )
    .orderBy(F.col("num_promo_codes").desc_nulls_first())
)

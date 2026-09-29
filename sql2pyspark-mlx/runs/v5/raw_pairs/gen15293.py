from pyspark.sql import functions as F

result = (
    df_promotions
    .cube(
        F.date_trunc("MONTH", "starts_on").alias("activity_month"),
        F.date_trunc("QUARTER", "starts_on").alias("quarter_start"),
    )
    .agg(
        F.stddev_samp("discount_pct").alias("discount_pct_stddev"),
        F.min("discount_pct").alias("min_discount_pct"),
        F.size(F.collect_set("promo_code")).alias("num_promo_codes"),
        F.count("*").alias("event_count"),
    )
    .orderBy(F.col("discount_pct_stddev").asc_nulls_last())
)

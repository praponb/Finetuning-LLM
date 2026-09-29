from pyspark.sql import functions as F

valid = (
    df_promotions
    .filter(F.col("discount_pct").between(0.19, 0.32))
    .withColumn("ends_on_minus_88d", F.date_sub("ends_on", 88))
    .withColumn("resolved_channel", F.coalesce("channel", F.lit("UNKNOWN")))
)

result = (
    valid
    .groupBy(F.date_trunc("YEAR", "ends_on_minus_88d").alias("year_start"))
    .agg(
        F.size(F.collect_set("promo_code")).alias("num_promo_codes"),
        F.count(F.when(F.col("channel") == "store", 1)).alias("store_count"),
        F.percentile_approx("discount_pct", [0.25, 0.5, 0.75]).alias("discount_pct_quartiles"),
        F.max(F.when(F.col("promo_code") == "SPRING10", F.col("discount_pct"))).alias("spring10_max_discount_pct"),
    )
    .filter(F.col("num_promo_codes") > 1)
    .orderBy(F.col("year_start").desc_nulls_first())
)

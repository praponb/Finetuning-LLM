from pyspark.sql import functions as F

result = (
    df_promotions
    .rollup(
        F.when(F.col("discount_pct") >= 0.34, "PREMIUM").otherwise("STANDARD").alias("discount_pct_tier"),
        "channel",
        F.date_trunc("YEAR", "ends_on").alias("year_start"),
    )
    .agg(
        F.count(F.when(F.col("channel") == "partner", 1)).alias("partner_count"),
        F.countDistinct("promo_code").alias("distinct_promo_codes"),
        F.count(F.when(F.col("promo_code") == "VIP", 1)).alias("vip_count"),
        F.stddev_samp("discount_pct").alias("discount_pct_stddev"),
        F.count("*").alias("cnt"),
        F.countDistinct("channel").alias("distinct_channels"),
    )
    .orderBy(F.col("year_start").desc())
)

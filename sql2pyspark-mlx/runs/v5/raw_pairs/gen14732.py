from pyspark.sql import functions as F

matched_keys = (
    df_promotions
    .select("channel", "promo_code")
    .exceptAll(
        df_promotions
        .filter(F.col("starts_on") > "2024-01-26")
        .select("channel", "promo_code")
    )
)

reconciled = (
    df_promotions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.channel") == F.col("k.channel")) & (F.col("s.promo_code") == F.col("k.promo_code")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(F.date_trunc("MONTH", "ends_on").alias("activity_month"))
    .agg(
        F.min("discount_pct").alias("min_discount_pct"),
        F.size(F.collect_set("channel")).alias("num_channels"),
        F.round(F.sum("discount_pct") / F.nullif(F.count(F.when(F.col("channel") == "partner", 1)), F.lit(0)), 2).alias("discount_pct_per_partner"),
        F.round(F.sum("discount_pct") / F.nullif(F.count(F.when(F.col("channel") == "mobile", 1)), F.lit(0)), 2).alias("discount_pct_per_mobile"),
        F.stddev_samp("discount_pct").alias("discount_pct_stddev"),
        F.round(F.sum("discount_pct") / F.nullif(F.count(F.when(F.col("promo_code") == "WELCOME5", 1)), F.lit(0)), 2).alias("discount_pct_per_welcome5"),
    )
    .filter(F.col("discount_pct_per_mobile") >= 0)
)

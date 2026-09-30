from pyspark.sql import functions as F

result = (
    df_promotions
    .cube("channel", "promo_code")
    .agg(
        F.size(F.collect_set("promo_code")).alias("num_promo_codes"),
        F.sum(F.when(F.col("promo_code") == "BUNDLE", F.col("discount_pct")).otherwise(0)).alias("bundle_discount_pct"),
        F.count("*").alias("row_count"),
        F.size(F.collect_set("channel")).alias("num_channels"),
        F.stddev_samp("discount_pct").alias("discount_pct_stddev"),
    )
    .orderBy("bundle_discount_pct")
)

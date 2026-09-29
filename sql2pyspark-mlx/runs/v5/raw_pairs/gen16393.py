from pyspark.sql import functions as F

result = (
    df_promotions
    .withColumn("discount_pct_tier", F.when(F.col("discount_pct") >= 0.3, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["discount_pct_tier", "promo_code"], ["discount_pct_tier"], []],
        "discount_pct_tier",
        "promo_code",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.countDistinct("promo_code").alias("distinct_promo_codes"),
        F.avg("discount_pct").alias("avg_discount_pct"),
        F.count_if(F.col("promo_code") == "BUNDLE").alias("bundle_events"),
        F.min("discount_pct").alias("min_discount_pct"),
        F.size(F.collect_set("promo_code")).alias("num_promo_codes"),
    )
    .filter(F.col("bundle_events") > 1)
    .orderBy(F.col("distinct_promo_codes").desc())
)

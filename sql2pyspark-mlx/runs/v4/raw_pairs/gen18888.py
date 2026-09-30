from pyspark.sql import functions as F

cleaned = (
    df_promotions
    .filter(F.col("ends_on") > F.lit("2024-03-26").cast("date"))
    .select(
        "id",
        "channel",
        "promo_code",
        "ends_on",
        "discount_pct",
        F.trim("channel").alias("clean_channel"),
        F.round("discount_pct", 1).alias("discount_pct_rounded"),
        F.coalesce("channel", F.lit("NONE")).alias("channel_filled"),
    )
)

result = (
    cleaned
    .withColumn("activity_month", F.date_trunc("MONTH", "ends_on"))
    .groupingSets(
        [["activity_month", "promo_code"], ["activity_month"], []],
        "activity_month",
        "promo_code",
    )
    .agg(
        F.percentile_approx("discount_pct_rounded", 0.95).alias("discount_pct_rounded_p95"),
        F.count(F.when(F.col("channel") == "partner", 1)).alias("partner_rows"),
        F.stddev_samp("discount_pct").alias("discount_pct_stddev"),
        F.percentile_approx("discount_pct", [0.25, 0.5, 0.75]).alias("discount_pct_quartiles"),
        F.countDistinct("promo_code").alias("distinct_promo_codes"),
        F.grouping("activity_month").alias("grouping_level"),
    )
    .filter(F.col("discount_pct_stddev") >= 0)
    .orderBy(F.col("discount_pct_stddev").desc_nulls_last())
)

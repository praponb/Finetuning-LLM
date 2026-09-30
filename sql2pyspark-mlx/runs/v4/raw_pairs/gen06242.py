from pyspark.sql import functions as F

result = (
    df_promotions
    .withColumn("discount_pct_tier", F.when(F.col("discount_pct") >= 0.29, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["discount_pct_tier", "channel"], ["discount_pct_tier"]],
        "discount_pct_tier",
        "channel",
    )
    .agg(
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.percentile_approx("discount_pct", 0.5).alias("discount_pct_p50"),
    )
    .orderBy(F.col("channel").desc())
)

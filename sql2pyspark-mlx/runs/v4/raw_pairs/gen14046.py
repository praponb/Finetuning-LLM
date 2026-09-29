from pyspark.sql import functions as F

cleaned = (
    df_promotions
    .filter((F.col("ends_on") >= F.lit("2023-11-10").cast("date")) & (F.col("ends_on") < "2023-09-23"))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        F.when(F.col("channel") == "store", 1).otherwise(0).alias("is_store"),
        F.coalesce("channel", F.lit("DIRECT")).alias("resolved_channel"),
    )
)

attributed = (
    cleaned
    .alias("a")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("a.channel"), F.lit("web")) == F.col("pr.channel")) & (F.col("a.starts_on") >= F.col("pr.starts_on")) & (F.col("a.starts_on") <= F.date_add(F.col("pr.ends_on"), 3)), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct"),
    )
)

cleaned_promotions = (
    attributed
    .filter((F.col("starts_on") > F.lit("2023-09-18").cast("date")) & F.col("channel").isNotNull())
    .select(
        "*",
        F.concat("promotion_promo_code", F.lit("/"), "channel").alias("promotion_promo_code_channel"),
        F.length("promotion_promo_code").alias("promotion_promo_code_length"),
    )
)

result = (
    cleaned_promotions
    .groupBy(F.date_trunc("QUARTER", "starts_on").alias("quarter_start"))
    .agg(
        F.stddev_samp("discount_pct").alias("discount_pct_stddev"),
        F.percentile_approx("is_store", 0.5).alias("is_store_p50"),
        F.sum("is_store").alias("total_is_store"),
    )
    .orderBy("discount_pct_stddev")
)

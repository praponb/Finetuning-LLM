from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("promo_code", "channel").orderBy(F.col("promo_id").desc(), "id", "promo_id")
w2 = Window.partitionBy("channel")
w3 = Window.orderBy("promo_id_bucket", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("channel_length", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_promotions
    .filter(F.col("starts_on") >= F.lit("2023-04-12").cast("date"))
    .select(
        "id",
        "channel",
        "promo_code",
        "ends_on",
        F.length("channel").alias("channel_length"),
    )
)

matched = (
    cleaned
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel") == F.col("pr.channel")) & (F.col("s.ends_on") >= F.col("pr.starts_on")) & (F.col("s.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct"),
    )
)

metrics = (
    matched
    .filter(F.col("ends_on") < "2024-08-01")
    .select(
        "id",
        "channel_length",
        "promo_id",
        "promotion_promo_code",
        F.ntile(10).over(w_running).alias("promo_id_bucket"),
        F.stddev_samp("discount_pct").over(w2).alias("channel_stddev_samp_discount_pct"),
    )
)

with_windows = (
    metrics
    .select(
        "id",
        "channel_length",
        "promo_id",
        "promotion_promo_code",
        "promo_id_bucket",
        "channel_stddev_samp_discount_pct",
        F.sum("channel_length").over(w3).alias("running_channel_length"),
        F.last("channel_stddev_samp_discount_pct", ignorenulls=True).over(w4).alias("last_known_channel_stddev_samp_discount_pct"),
    )
)

kept_keys = (
    with_windows
    .select("id")
    .intersectAll(
        df_promotions
        .filter(F.col("discount_pct") > 0.17)
        .select("id")
    )
)

verified = (
    with_windows
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

kept_keys_2 = (
    verified
    .select("id")
    .subtract(
        df_promotions
        .filter(F.col("channel") == "mobile")
        .select("id")
    )
)

reconciled = (
    verified
    .alias("s")
    .join(kept_keys_2.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        F.when(F.col("last_known_channel_stddev_samp_discount_pct") >= 0, "PREMIUM").otherwise("STANDARD").alias("last_known_channel_stddev_samp_discount_pct_tier"),
    )
    .agg(
        F.percentile_approx("channel_length", 0.5).alias("channel_length_p50"),
        F.round(F.sum("promo_id") / F.nullif(F.count(F.when(F.col("promotion_promo_code") == "BUNDLE", 1)), F.lit(0)), 2).alias("promo_id_per_bundle"),
        F.count(F.when(F.col("promotion_promo_code") == "SPRING10", 1)).alias("spring10_rows"),
        F.size(F.collect_set("promotion_promo_code")).alias("num_promotion_promo_codes"),
    )
    .orderBy(F.col("num_promotion_promo_codes").desc_nulls_last())
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("quarter_start").orderBy(F.col("starts_on_day_p50").desc())

cleaned = (
    df_promotions
    .filter(F.col("channel").isNotNull() & F.col("channel").isin(["store", "web"]))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "discount_pct",
        F.dayofmonth("starts_on").alias("starts_on_day"),
        F.coalesce("channel", F.lit("DIRECT")).alias("resolved_channel"),
    )
)

matched_keys = (
    cleaned
    .select("id", "channel")
    .intersectAll(
        df_promotions
        .filter(F.col("channel").isin(["store", "mobile", "web"]))
        .select("id", "channel")
    )
)

verified = (
    cleaned
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

attributed = (
    verified
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel") == F.col("pr.channel")) & (F.col("e.starts_on") >= F.col("pr.starts_on")) & (F.col("e.starts_on") <= F.date_add(F.col("pr.ends_on"), 10)), "inner")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

matched_keys_2 = (
    attributed
    .select("promo_code")
    .intersect(
        df_promotions
        .filter(~F.col("promo_code").isin(["WELCOME5", "SPRING10"]))
        .select("promo_code")
    )
)

reconciled = (
    attributed
    .alias("s")
    .join(matched_keys_2.alias("k"), (F.col("s.promo_code") == F.col("k.promo_code")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        F.date_trunc("QUARTER", "starts_on").alias("quarter_start"),
        F.date_trunc("MONTH", "starts_on").alias("activity_month"),
    )
    .agg(
        F.percentile_approx("starts_on_day", [0.25, 0.5, 0.75]).alias("starts_on_day_quartiles"),
        F.percentile_approx("starts_on_day", 0.5).alias("starts_on_day_p50"),
        F.sum(F.when(F.col("promotion_promo_code") == "WELCOME5", F.col("promo_id")).otherwise(0)).alias("welcome5_promo_id"),
        F.sum(F.when(F.col("promotion_promo_code") == "VIP", F.col("promo_id")).otherwise(0)).alias("vip_promo_id"),
        F.percentile_approx("discount_pct", [0.25, 0.5, 0.75]).alias("discount_pct_quartiles"),
    )
    .filter(F.col("vip_promo_id") < 0)
    .withColumn("starts_on_day_p50_dense_rank", F.dense_rank().over(w))
    .orderBy("vip_promo_id")
)

from pyspark.sql import functions as F

filtered = (
    df_promotions
    .filter(F.col("starts_on") >= F.lit("2023-09-05").cast("date"))
    .select(
        "id",
        "channel",
        "starts_on",
        "ends_on",
        F.substring("promo_code", 1, 4).alias("promo_code_prefix"),
        F.coalesce("channel", F.lit("OTHER")).alias("channel_filled"),
    )
)

matched = (
    filtered
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel") == F.col("pr.channel")) & F.col("src.ends_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "inner")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

matched_promotions = (
    matched
    .alias("src")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("src.channel"), F.lit("store")) == F.col("pr.channel")) & (F.col("src.starts_on") >= F.col("pr.starts_on")) & (F.col("src.starts_on") < F.col("pr.ends_on") + F.expr("INTERVAL 14 DAYS")), "inner")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

with_window = (
    matched_promotions
    .alias("src")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("src.channel"), F.lit("store")) == F.col("pr.channel")) & (F.col("src.ends_on") >= F.col("pr.starts_on")) & (F.col("src.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id_3"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

result = (
    with_window
    .groupBy("channel")
    .pivot("promotion_promo_code", ["WELCOME5", "BUNDLE", "VIP"])
    .agg(F.sum("promo_id_3").alias("value_sum"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "channel",
        F.col("WELCOME5_value_sum").alias("welcome5_value_sum"),
        F.col("WELCOME5_cnt").alias("welcome5_cnt"),
        F.col("BUNDLE_value_sum").alias("bundle_value_sum"),
        F.col("BUNDLE_cnt").alias("bundle_cnt"),
        F.col("VIP_value_sum").alias("vip_value_sum"),
        F.col("VIP_cnt").alias("vip_cnt"),
    )
)

from pyspark.sql import functions as F

cleaned = (
    df_promotions
    .filter(F.col("starts_on") < "2025-10-01")
    .select(
        "id",
        "channel",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.lower("channel").alias("channel_lower"),
        F.concat("channel", F.lit("/"), "promo_code").alias("channel_promo_code"),
    )
)

matched = (
    cleaned
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel") == F.col("pr.channel")) & (F.col("s.starts_on") >= F.col("pr.starts_on")) & (F.col("s.starts_on") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

matched_promotions = (
    matched
    .alias("src")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("src.channel"), F.lit("store")) == F.col("pr.channel")) & F.col("src.ends_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

remaining_keys = (
    matched_promotions
    .select("id", "channel")
    .intersectAll(
        df_promotions
        .filter(F.col("discount_pct").between(0.16, 0.25))
        .select("id", "channel")
    )
)

kept_rows = (
    matched_promotions
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

attributed = (
    kept_rows
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel") == F.col("pr.channel")) & F.col("src.ends_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id_3"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_3"),
    )
)

result = (
    attributed
    .select("channel", "promotion_discount_pct", "promotion_discount_pct_3")
    .unpivot(
        ids=["channel"],
        values=["promotion_discount_pct", "promotion_discount_pct_3"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)

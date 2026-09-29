from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("promotion_promo_code").orderBy("is_partner", "id", "promo_id").rowsBetween(-2, Window.currentRow)

valid = (
    df_promotions
    .filter(F.col("starts_on") >= F.to_date(F.lit("2023-10-02")))
    .select(
        "*",
        F.datediff("starts_on", "ends_on").alias("days_ends_on_to_starts_on"),
        F.when(F.col("channel") == "partner", 1).otherwise(0).alias("is_partner"),
    )
)

filtered = (
    valid
    .filter(F.col("days_ends_on_to_starts_on").between(48, 54) & F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "promo_code",
        "ends_on",
        "discount_pct",
        "days_ends_on_to_starts_on",
        "is_partner",
        F.last_day("starts_on").alias("starts_on_month_end"),
        F.col("discount_pct").cast("int").alias("discount_pct_int"),
        F.coalesce("channel", F.lit("N/A")).alias("channel_filled"),
    )
)

matched = (
    filtered
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("a.starts_on_month_end") >= F.col("pr.starts_on")) & (F.col("a.starts_on_month_end") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "inner")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

metrics = (
    matched
    .withColumn("last3_count_discount_pct", F.count("discount_pct").over(w_part))
    .withColumn("last3_avg_discount_pct", F.avg("discount_pct").over(w_part))
    .withColumn("last3_min_discount_pct", F.min("discount_pct").over(w_part))
    .withColumn("last3_sum_discount_pct", F.sum("discount_pct").over(w_part))
)

remaining_keys = (
    metrics
    .select("channel")
    .subtract(
        df_promotions
        .filter(F.col("discount_pct").between(0.18, 0.31))
        .select("channel")
    )
)

verified = (
    metrics
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        F.when(F.col("discount_pct") >= 0.3, "PREMIUM").otherwise("STANDARD").alias("discount_pct_tier"),
        "promotion_promo_code",
    )
    .agg(
        F.percentile_approx("promo_id", 0.9).alias("promo_id_p90"),
        F.countDistinct("channel").alias("distinct_channels"),
        F.avg(F.when(F.col("channel") == "web", F.col("last3_sum_discount_pct"))).alias("web_avg_last3_sum_discount_pct"),
        F.size(F.collect_set("promotion_promo_code")).alias("num_promotion_promo_codes"),
    )
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("ends_on").desc(), "id")
w2 = Window.partitionBy("channel").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("channel")
w4 = Window.orderBy(F.col("promo_id").desc_nulls_first())
w5 = Window.partitionBy("promotion_promo_code_2")
w6 = Window.orderBy(F.col("discount_pct_rounded").desc_nulls_first())

enriched = (
    df_promotions
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("first_known_channel", F.first("channel", ignorenulls=True).over(w2))
    .withColumn("channel_min_discount_pct", F.min("discount_pct").over(w3))
)

base = (
    enriched
    .filter(F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        "seq_no",
        "channel_min_discount_pct",
        F.round("discount_pct", 0).alias("discount_pct_rounded"),
        F.date_format("ends_on", "yyyy-MM-dd").alias("ends_on_label"),
    )
)

matched_keys = (
    base
    .select("id", "channel")
    .subtract(
        df_promotions
        .filter(F.col("promo_code") == "SPRING10")
        .select("id", "channel")
    )
)

reconciled = (
    base
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

with_window = (
    reconciled
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("a.ends_on") >= F.col("pr.starts_on")) & (F.col("a.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

matched = (
    with_window
    .alias("s")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("s.channel"), F.lit("web")) == F.col("pr.channel")) & F.col("s.starts_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

metrics = (
    matched
    .withColumn("promo_id_rank", F.rank().over(w4))
    .withColumn("promotion_promo_code_2_stddev_samp_seq_no", F.stddev_samp("seq_no").over(w5))
    .withColumn("discount_pct_rounded_cume_dist", F.cume_dist().over(w6))
    .withColumn("promotion_discount_pct_share", F.round((F.col("promotion_discount_pct") / F.sum("promotion_discount_pct").over(w5)), 4))
)

with_window_promotions = (
    metrics
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel") == F.col("pr.channel")) & (F.col("src.ends_on") >= F.col("pr.starts_on")) & (F.col("src.ends_on") <= F.date_add(F.col("pr.ends_on"), 10)), "inner")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id_3"),
        F.col("pr.promo_code").alias("promotion_promo_code_3"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_3"),
    )
)

result = (
    with_window_promotions
    .select(
        "promo_code",
        F.col("promotion_discount_pct").alias("PROMOTION_DISCOUNT_PCT"),
        "promotion_discount_pct_2",
        "discount_pct_rounded",
        F.col("promotion_discount_pct_share").alias("PROMOTION_DISCOUNT_PCT_SHARE_VAL"),
    )
    .unpivot(
        ["promo_code"],
        ["PROMOTION_DISCOUNT_PCT", "promotion_discount_pct_2", "discount_pct_rounded", "PROMOTION_DISCOUNT_PCT_SHARE_VAL"],
        "metric_name",
        "value",
    )
    .filter(F.col("value").isNotNull())
)

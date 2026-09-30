from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promotion_promo_code").orderBy(F.col("promo_id").desc_nulls_first(), "id", "promo_id")
w2 = Window.partitionBy("promotion_promo_code").orderBy("is_store", "id", "promo_id")

base = (
    df_promotions
    .filter(F.col("discount_pct").between(0.16, 0.31))
    .withColumn("is_store", F.when(F.col("channel") == "store", 1).otherwise(0))
    .withColumn("channel_clean", F.coalesce("channel", F.lit("OTHER")))
)

matched = (
    base
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel").eqNullSafe(F.col("pr.channel"))) & F.col("src.ends_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

deduped = (
    matched
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

filtered = (
    deduped
    .filter(F.col("channel").isNotNull() & (F.col("channel").eqNullSafe(F.lit("store"))))
    .select(
        "*",
        F.lower("promotion_promo_code").alias("promotion_promo_code_lower"),
        F.last_day("starts_on").alias("starts_on_month_end"),
        F.coalesce("promotion_promo_code", F.lit("DIRECT")).alias("promotion_promo_code_filled"),
    )
)

with_windows = (
    filtered
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "discount_pct",
        "channel_clean",
        "promo_id",
        "promotion_discount_pct",
        "promotion_promo_code_lower",
        "starts_on_month_end",
        F.lag("promo_id").over(w2).alias("prev_promo_id"),
        F.lead("promotion_discount_pct").over(w2).alias("next_promotion_discount_pct"),
        F.lag("promotion_discount_pct").over(w2).alias("prev_promotion_discount_pct"),
        F.row_number().over(w2).alias("seq_no"),
    )
)

remaining_keys = (
    with_windows
    .select("id", "channel")
    .subtract(
        df_promotions
        .filter(F.col("channel").isin(["mobile", "store", "partner"]))
        .select("id", "channel")
    )
)

kept_rows = (
    with_windows
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

grouped = (
    kept_rows
    .groupBy(
        F.date_trunc("MONTH", "starts_on").alias("activity_month"),
        F.date_trunc("QUARTER", "starts_on_month_end").alias("quarter_start"),
    )
    .agg(
        F.avg(F.when(F.col("channel") == "store", F.col("promo_id"))).alias("store_avg_promo_id"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("distinct_ids") >= 1)
)

result = (
    grouped
    .select("activity_month", "quarter_start", "store_avg_promo_id")
    .orderBy(F.col("store_avg_promo_id").desc_nulls_last(), "quarter_start")
)

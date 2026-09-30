from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.orderBy("days_starts_on_to_ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.orderBy("discount_pct")
w_ordered_2 = Window.partitionBy("promo_code", "channel").orderBy("discount_pct", "id")
w4 = Window.partitionBy("promo_code").orderBy("prev_discount_pct", "id")
w5 = Window.partitionBy("promotion_promo_code", "channel").orderBy(F.col("discount_pct").desc())
w6 = Window.orderBy(F.col("discount_pct_percent_rank").asc_nulls_first())
w7 = Window.partitionBy("promo_code")
w_part = Window.partitionBy("channel").orderBy("prev_discount_pct")
w9 = Window.partitionBy("channel")

base = (
    df_promotions
    .filter(F.col("starts_on").between("2024-08-01", "2024-11-01"))
    .withColumn("channel_title", F.initcap(F.trim("channel")))
    .withColumn("days_starts_on_to_ends_on", F.datediff("ends_on", "starts_on"))
    .withColumn("channel_clean", F.coalesce("channel", F.lit("NONE")))
)

scored = (
    base
    .select(
        "*",
        F.sum("days_starts_on_to_ends_on").over(w_ordered).alias("running_days_starts_on_to_ends_on"),
        F.percent_rank().over(w_hist).alias("discount_pct_percent_rank"),
        F.lag("discount_pct", 1).over(w_ordered_2).alias("prev_discount_pct"),
    )
)

deduped = (
    scored
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
)

attributed = (
    deduped
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel") == F.col("pr.channel")) & F.col("e.starts_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

latest = (
    attributed
    .withColumn("dedup_rank_2", F.rank().over(w5))
    .filter(F.col("dedup_rank_2") == 1)
)

kept_keys = (
    latest
    .select("id", "channel")
    .intersectAll(
        df_promotions
        .filter(F.month("ends_on").isin([1, 9, 11]))
        .select("id", "channel")
    )
)

verified = (
    latest
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

scored_promotions = (
    verified
    .select(
        "*",
        F.percent_rank().over(w6).alias("discount_pct_percent_rank_percent_rank"),
        F.round((F.col("promotion_discount_pct") / F.sum("promotion_discount_pct").over(w7)), 4).alias("promotion_discount_pct_share"),
        F.percent_rank().over(w_part).alias("prev_discount_pct_percent_rank"),
        F.stddev_samp("days_starts_on_to_ends_on").over(w9).alias("channel_stddev_samp_days_starts_on_to_ends_on"),
    )
)

result = (
    scored_promotions
    .groupBy("promo_code", "promotion_promo_code")
    .pivot("channel", ["mobile", "partner"])
    .agg(
        F.sum("days_starts_on_to_ends_on").alias("value_sum"),
        F.count(F.lit(1)).alias("cnt"),
    )
)

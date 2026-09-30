from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code", "channel").orderBy("ends_on_plus_months", "id")
w_recent = Window.partitionBy("promo_code", "channel")
w_grp = Window.partitionBy("channel")
w_part = Window.partitionBy("promo_code")
w5 = Window.partitionBy("promo_code").orderBy(F.col("starts_on_dayofweek").asc_nulls_last())

base = (
    df_promotions
    .filter(F.col("channel").isNotNull() & (F.col("starts_on") < "2023-11-01"))
    .select(
        "id",
        "channel",
        "promo_code",
        "ends_on",
        F.add_months("ends_on", 6).alias("ends_on_plus_months"),
        F.dayofweek("starts_on").alias("starts_on_dayofweek"),
    )
)

latest = (
    base
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

matched_keys = (
    latest
    .select("channel")
    .intersectAll(
        df_promotions
        .filter(F.col("discount_pct") <= 0.19)
        .select("channel")
    )
)

verified = (
    latest
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .select(
        "*",
        F.min("starts_on_dayofweek").over(w_recent).alias("promo_code_min_starts_on_dayofweek"),
        F.max("starts_on_dayofweek").over(w_grp).alias("channel_max_starts_on_dayofweek"),
    )
)

with_windows = (
    enriched
    .withColumn("promo_code_min_promo_code_min_starts_on_dayofweek", F.min("promo_code_min_starts_on_dayofweek").over(w_part))
    .withColumn("promo_code_count_promo_code_min_starts_on_dayofweek", F.count("promo_code_min_starts_on_dayofweek").over(w_recent))
    .withColumn("starts_on_dayofweek_rank", F.rank().over(w5))
    .withColumn("promo_code_max_starts_on_dayofweek", F.max("starts_on_dayofweek").over(w_part))
    .withColumn("promo_code_max_promo_code_min_starts_on_dayofweek", F.max("promo_code_min_starts_on_dayofweek").over(w_part))
)

result = (
    with_windows
    .groupBy(F.date_trunc("QUARTER", "ends_on").alias("quarter_start"), "channel")
    .agg(
        F.percentile_approx("promo_code_min_promo_code_min_starts_on_dayofweek", [0.25, 0.5, 0.75]).alias("promo_code_min_promo_code_min_starts_on_dayofweek_quartiles"),
        F.round(F.sum("promo_code_min_starts_on_dayofweek") / F.nullif(F.count(F.when(F.col("channel") == "mobile", 1)), F.lit(0)), 2).alias("promo_code_min_starts_on_dayofweek_per_mobile"),
        F.round(F.avg("promo_code_min_promo_code_min_starts_on_dayofweek"), 2).alias("avg_promo_code_min_promo_code_min_starts_on_dayofweek"),
        F.count_if(F.col("channel") == "partner").alias("partner_events"),
    )
    .filter(F.col("avg_promo_code_min_promo_code_min_starts_on_dayofweek") > 0)
)

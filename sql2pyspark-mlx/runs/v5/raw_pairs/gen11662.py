from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("channel").orderBy("discount_pct", "id")
w2 = Window.orderBy("starts_on", "id")
w3 = Window.orderBy("ends_on_plus_83d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("promo_code")
w5 = Window.orderBy(F.col("next_discount_pct").desc_nulls_last())
w6 = Window.partitionBy("promo_code").orderBy(F.col("promo_code_avg_discount_pct").asc_nulls_last())

base = (
    df_promotions
    .filter(F.col("discount_pct") < 0.18)
    .select(
        "*",
        F.date_add("ends_on", 83).alias("ends_on_plus_83d"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("channel_clean"),
    )
)

metrics = (
    base
    .select(
        "id",
        "promo_code",
        "ends_on",
        "discount_pct",
        "channel_clean",
        F.lead("discount_pct", 1).over(w_part).alias("next_discount_pct"),
        F.row_number().over(w2).alias("event_seq"),
        F.last("channel", ignorenulls=True).over(w3).alias("last_known_channel"),
    )
)

remaining_keys = (
    metrics
    .select("promo_code", "id")
    .intersectAll(
        df_promotions
        .filter(~F.col("promo_code").isin(["SPRING10", "WELCOME5"]))
        .select("promo_code", "id")
    )
)

kept_rows = (
    metrics
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.promo_code") == F.col("k.promo_code")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_windows = (
    kept_rows
    .filter(F.col("next_discount_pct") <= 0.2)
    .withColumn("promo_code_avg_discount_pct", F.avg("discount_pct").over(w4))
)

scored = (
    with_windows
    .select(
        "id",
        "promo_code",
        "discount_pct",
        "next_discount_pct",
        "last_known_channel",
        "promo_code_avg_discount_pct",
        F.avg("event_seq").over(w4).alias("promo_code_avg_event_seq"),
        F.dense_rank().over(w5).alias("next_discount_pct_dense_rank"),
        F.round((F.col("event_seq") / F.sum("event_seq").over(w4)), 4).alias("event_seq_share"),
        F.rank().over(w6).alias("promo_code_avg_discount_pct_rank"),
        F.count("discount_pct").over(w4).alias("promo_code_count_discount_pct"),
    )
)

scored_promotions = (
    scored
    .select(
        "id",
        "promo_code_avg_discount_pct",
        "next_discount_pct_dense_rank",
        F.round((F.col("next_discount_pct") / F.sum("next_discount_pct").over(w4)), 4).alias("next_discount_pct_share"),
        F.rank().over(w5).alias("next_discount_pct_rank"),
        F.avg("next_discount_pct").over(w4).alias("promo_code_avg_next_discount_pct"),
    )
)

filtered = (
    scored_promotions
    .filter(F.col("next_discount_pct_rank") > 8)
    .select(
        "*",
        (F.col("next_discount_pct_share") - F.col("next_discount_pct_dense_rank")).alias("next_discount_pct_share_minus_next_discount_pct_dense_rank"),
        F.round((F.col("next_discount_pct_share") / F.col("next_discount_pct_rank")), 2).alias("next_discount_pct_share_per_next_discount_pct_rank"),
    )
)

result = (
    filtered
    .select(
        "promo_code_avg_discount_pct",
        "next_discount_pct_dense_rank",
        "next_discount_pct_share_minus_next_discount_pct_dense_rank",
        "next_discount_pct_share_per_next_discount_pct_rank",
    )
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy(F.col("discount_pct").desc(), "id")
w2 = Window.partitionBy("channel", "promo_code").orderBy("rank_in_group")
w3 = Window.partitionBy("promo_code", "channel").orderBy("rank_in_group", "id")
w4 = Window.partitionBy("promo_code").orderBy("rank_in_group", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("channel").orderBy(F.col("rank_in_group").desc(), "id")

base = (
    df_promotions
    .filter(F.col("starts_on") > F.lit("2023-09-11").cast("date"))
    .select(
        "id",
        "channel",
        "promo_code",
        "discount_pct",
        F.initcap(F.trim("channel")).alias("channel_title"),
    )
)

ranked = (
    base
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

deduped = (
    ranked
    .withColumn("rank_in_group_2", F.rank().over(w2))
    .filter(F.col("rank_in_group_2") == 1)
)

with_windows = (
    deduped
    .select(
        "id",
        "channel",
        "rank_in_group",
        "rank_in_group_2",
        F.lead("discount_pct", 1).over(w3).alias("next_discount_pct"),
        F.sum("discount_pct").over(w4).alias("cumulative_discount_pct"),
    )
)

cleaned = (
    with_windows
    .filter(F.col("rank_in_group_2") > 1)
    .withColumn("rank_in_group_rounded", F.round("rank_in_group", 1))
)

cleaned_promotions = (
    cleaned
    .filter(F.col("channel").isNotNull() & F.col("next_discount_pct").between(0.18, 0.22))
    .select(
        "id",
        "channel",
        "rank_in_group",
        "rank_in_group_2",
        "next_discount_pct",
        "rank_in_group_rounded",
        (F.col("rank_in_group_rounded") * F.col("next_discount_pct")).alias("rank_in_group_rounded_times_next_discount_pct"),
        F.coalesce("channel", F.lit("NONE")).alias("channel_clean"),
    )
)

deduped_promotions = (
    cleaned_promotions
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
)

result = (
    deduped_promotions
    .select(
        "id",
        "rank_in_group",
        "rank_in_group_2",
        "next_discount_pct",
        "rank_in_group_rounded",
        "channel_clean",
        "rn",
    )
    .orderBy(F.col("rank_in_group").asc_nulls_last())
)

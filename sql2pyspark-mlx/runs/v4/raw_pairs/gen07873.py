from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("discount_pct_rounded", "id").rowsBetween(-5, -1)
w2 = Window.partitionBy("channel").orderBy(F.col("discount_pct").desc())
w3 = Window.partitionBy("promo_code").orderBy("discount_pct", "id")
w4 = Window.orderBy(F.col("discount_pct").desc())
w5 = Window.partitionBy("promo_code").orderBy(F.col("discount_pct_rounded").desc(), "id", "promo_id")
w6 = Window.partitionBy("channel").orderBy("promotion_discount_pct", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("promotion_promo_code").orderBy(F.col("cumulative_promo_id").asc_nulls_first(), "id", "promo_id")
w8 = Window.partitionBy("promotion_promo_code", "promo_code").orderBy(F.col("promo_id").desc(), "id", "promo_id")
w9 = Window.orderBy(F.col("discount_pct_rounded_bucket").desc(), "id", "promo_id")
w10 = Window.orderBy(F.col("cumulative_promo_id").desc(), "id", "promo_id")

filtered = (
    df_promotions
    .filter((F.col("ends_on") > F.lit("2024-05-27").cast("date")) & F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.concat_ws("/", "promo_code", "channel").alias("promo_code_channel_key"),
        F.round("discount_pct", 1).alias("discount_pct_rounded"),
        F.coalesce("channel", F.lit("OTHER")).alias("resolved_channel"),
    )
)

with_windows = (
    filtered
    .select(
        "*",
        F.avg("discount_pct").over(w).alias("discount_pct_avg_5_prior"),
        F.percent_rank().over(w2).alias("discount_pct_percent_rank"),
        F.lead("discount_pct_rounded", 1).over(w3).alias("next_discount_pct_rounded"),
    )
)

attributed = (
    with_windows
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel") == F.col("pr.channel")) & (F.col("e.ends_on") >= F.col("pr.starts_on")) & (F.col("e.ends_on") <= F.date_add(F.col("pr.ends_on"), 10)), "inner")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

with_windows_promotions = (
    attributed
    .withColumn("discount_pct_percent_rank_2", F.percent_rank().over(w4))
    .withColumn("discount_pct_rounded_bucket", F.ntile(3).over(w5))
    .withColumn("cumulative_promo_id", F.sum("promo_id").over(w6))
)

latest = (
    with_windows_promotions
    .withColumn("row_num", F.row_number().over(w7))
    .filter(F.col("row_num") == 1)
)

scored = (
    latest
    .select(
        "*",
        F.row_number().over(w8).alias("event_seq"),
        F.row_number().over(w9).alias("seq_no"),
        F.row_number().over(w10).alias("seq_no_2"),
    )
)

result = (
    scored
    .select(
        "id",
        "ends_on",
        "discount_pct",
        "promo_code_channel_key",
        "resolved_channel",
        "discount_pct_avg_5_prior",
        "discount_pct_percent_rank",
        "next_discount_pct_rounded",
        "promo_id",
    )
)

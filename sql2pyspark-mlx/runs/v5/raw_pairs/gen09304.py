from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("ends_on", "id")
w2 = Window.partitionBy("promo_code").orderBy("discount_pct")
w_running = Window.orderBy("discount_pct_rank", "id")
w4 = Window.orderBy("starts_on", "id").rowsBetween(-5, -1)
w5 = Window.partitionBy("channel").orderBy("discount_pct", "id")
w6 = Window.orderBy("ends_on", "id").rowsBetween(-6, -1)
w7 = Window.partitionBy("promo_code", "channel").orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("channel", "promo_code").orderBy("ends_on", "id")
w9 = Window.partitionBy("channel").orderBy(F.col("discount_pct_rank_whole").desc_nulls_first(), "id")

enriched = (
    df_promotions
    .select(
        "*",
        F.lag("discount_pct", 1).over(w).alias("prev_discount_pct"),
        F.rank().over(w2).alias("discount_pct_rank"),
    )
)

scored = (
    enriched
    .select(
        "*",
        F.ntile(5).over(w_running).alias("discount_pct_rank_bucket"),
        F.avg("discount_pct").over(w4).alias("discount_pct_avg_5_prior"),
        F.row_number().over(w5).alias("seq_no"),
        F.avg("discount_pct_rank").over(w6).alias("discount_pct_rank_avg_6_prior"),
        F.sum("discount_pct").over(w7).alias("running_discount_pct"),
    )
)

ranked = (
    scored
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "channel",
        "discount_pct",
        "prev_discount_pct",
        "discount_pct_rank",
        "discount_pct_avg_5_prior",
    )
)

valid = (
    ranked
    .filter(F.col("channel") != "partner")
    .select(
        "id",
        "channel",
        "discount_pct",
        "prev_discount_pct",
        "discount_pct_rank",
        "discount_pct_avg_5_prior",
        F.ceil("discount_pct_rank").alias("discount_pct_rank_whole"),
        F.initcap(F.trim("channel")).alias("channel_title"),
    )
)

latest = (
    valid
    .withColumn("row_num", F.row_number().over(w9))
    .filter(F.col("row_num") == 1)
)

matched_keys = (
    latest
    .select("id", "channel")
    .intersect(
        df_promotions
        .filter(F.col("promo_code").isin(["VIP", "BUNDLE"]))
        .select("id", "channel")
    )
)

reconciled = (
    latest
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "id",
        "channel",
        "prev_discount_pct",
        "discount_pct_rank",
        "discount_pct_avg_5_prior",
        "discount_pct_rank_whole",
        "row_num",
    )
    .orderBy(F.col("channel").desc_nulls_first())
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("channel", "promo_code").orderBy(F.col("discount_pct").desc())
w2 = Window.partitionBy("channel")
w3 = Window.partitionBy("promo_code")
w4 = Window.partitionBy("promo_code", "channel")
w5 = Window.partitionBy("promo_code").orderBy(F.col("discount_pct_dense_rank").desc_nulls_first())
w6 = Window.partitionBy("promo_code").orderBy(F.col("discount_pct").desc())

cleaned = (
    df_promotions
    .filter((F.col("ends_on") >= F.lit("2024-03-13").cast("date")) & F.col("channel").isNotNull())
    .select(
        "*",
        F.initcap(F.trim("channel")).alias("channel_title"),
        F.coalesce("channel", F.lit("DIRECT")).alias("channel_filled"),
    )
)

matched_keys = (
    cleaned
    .select("id", "promo_code")
    .subtract(
        df_promotions
        .filter(F.col("ends_on").between("2023-07-01", "2024-07-26"))
        .select("id", "promo_code")
    )
)

verified = (
    cleaned
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.promo_code") == F.col("k.promo_code")), "inner")
    .select("s.*")
)

with_windows = (
    verified
    .select(
        "*",
        F.dense_rank().over(w_hist).alias("discount_pct_dense_rank"),
        F.stddev_samp("discount_pct").over(w2).alias("channel_stddev_samp_discount_pct"),
    )
)

scored = (
    with_windows
    .withColumn("promo_code_avg_channel_stddev_samp_discount_pct", F.avg("channel_stddev_samp_discount_pct").over(w3))
    .withColumn("channel_stddev_samp_discount_pct_share", F.round((F.col("channel_stddev_samp_discount_pct") / F.sum("channel_stddev_samp_discount_pct").over(w4)), 4))
    .withColumn("discount_pct_dense_rank_cume_dist", F.cume_dist().over(w5))
    .withColumn("discount_pct_dense_rank_2", F.dense_rank().over(w6))
)

result = (
    scored
    .groupBy("channel")
    .pivot("promo_code", ["BUNDLE", "VIP", "SPRING10"])
    .agg(F.sum("promo_code_avg_channel_stddev_samp_discount_pct"))
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code", "channel").orderBy(F.col("ends_on").desc(), "id")
w2 = Window.partitionBy("promo_code").orderBy(F.col("discount_pct").desc())
w3 = Window.partitionBy("channel", "promo_code").orderBy(F.col("ends_on").desc(), "id")
w4 = Window.partitionBy("promo_code").orderBy(F.col("event_seq_2").desc_nulls_last())
w5 = Window.partitionBy("promo_code")
w6 = Window.partitionBy("channel").orderBy(F.col("num_channels").desc())

scored = (
    df_promotions
    .select(
        "*",
        F.row_number().over(w).alias("event_seq"),
        F.cume_dist().over(w2).alias("discount_pct_cume_dist"),
        F.row_number().over(w3).alias("event_seq_2"),
    )
)

remaining_keys = (
    scored
    .select("id", "channel")
    .exceptAll(
        df_promotions
        .filter(F.col("channel") == "mobile")
        .select("id", "channel")
    )
)

reconciled = (
    scored
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

with_windows = (
    reconciled
    .withColumn("event_seq_2_cume_dist", F.cume_dist().over(w4))
    .withColumn("promo_code_avg_event_seq_2", F.avg("event_seq_2").over(w5))
)

with_window = (
    with_windows
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel") == F.col("pr.channel")) & (F.col("a.starts_on") >= F.col("pr.starts_on")) & (F.col("a.starts_on") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

summary = (
    with_window
    .groupBy("channel", F.date_trunc("WEEK", "ends_on").alias("week_start"))
    .agg(
        F.round(F.avg("event_seq_2"), 2).alias("avg_event_seq_2"),
        F.avg("discount_pct_cume_dist").alias("avg_discount_pct_cume_dist"),
        F.count(F.when(F.col("channel") == "mobile", 1)).alias("mobile_rows"),
        F.percentile_approx("discount_pct_cume_dist", 0.5).alias("discount_pct_cume_dist_p50"),
        F.round(F.avg("promo_code_avg_event_seq_2"), 2).alias("avg_promo_code_avg_event_seq_2"),
        F.size(F.collect_set("channel")).alias("num_channels"),
    )
    .filter(F.col("avg_promo_code_avg_event_seq_2") > 0)
    .withColumn("num_channels_dense_rank", F.dense_rank().over(w6))
)

result = (
    summary
    .select(
        "channel",
        "week_start",
        "avg_discount_pct_cume_dist",
        "mobile_rows",
        "discount_pct_cume_dist_p50",
        "avg_promo_code_avg_event_seq_2",
        "num_channels",
        "num_channels_dense_rank",
    )
    .orderBy(F.col("num_channels").desc_nulls_first())
)

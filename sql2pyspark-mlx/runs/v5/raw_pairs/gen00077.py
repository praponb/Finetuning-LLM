from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code")
w2 = Window.partitionBy("promo_code").orderBy(F.col("ends_on").desc(), "id", "promo_id")
w3 = Window.orderBy("discount_pct", "id", "promo_id")
w4 = Window.orderBy("ends_on", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("promotion_promo_code").orderBy(F.col("promotion_discount_pct").desc_nulls_first(), "id", "promo_id")
w_running = Window.partitionBy("promo_code", "channel").orderBy("ends_on", "id", "promo_id")
w_hist = Window.partitionBy("channel", "promo_code").orderBy(F.col("promo_id").desc(), "id", "promo_id")
w8 = Window.partitionBy("promo_code").orderBy("next_promotion_discount_pct", "id", "promo_id")
w9 = Window.partitionBy("promo_code", "channel")
w10 = Window.orderBy("promo_id", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_window = (
    df_promotions
    .alias("e")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("e.channel"), F.lit("web")) == F.col("pr.channel")) & (F.col("e.ends_on") >= F.col("pr.starts_on")) & (F.col("e.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

with_windows = (
    with_window
    .withColumn("promo_code_count_promotion_discount_pct", F.count("promotion_discount_pct").over(w))
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("promo_code_stddev_samp_promotion_discount_pct", F.stddev_samp("promotion_discount_pct").over(w))
    .withColumn("discount_pct_bucket", F.ntile(3).over(w3))
    .withColumn("cumulative_promo_id", F.sum("promo_id").over(w4))
)

deduped = (
    with_windows
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "channel",
        "promo_code",
        "ends_on",
        "promo_id",
        "promotion_discount_pct",
        "cumulative_promo_id",
    )
)

enriched = (
    deduped
    .select(
        "id",
        "channel",
        "promo_code",
        "ends_on",
        "promo_id",
        "promotion_discount_pct",
        F.lead("cumulative_promo_id").over(w_running).alias("next_cumulative_promo_id"),
        F.lag("promotion_discount_pct").over(w_running).alias("prev_promotion_discount_pct"),
        F.lead("promotion_discount_pct").over(w_running).alias("next_promotion_discount_pct"),
        F.row_number().over(w_running).alias("seq_no"),
        F.row_number().over(w_hist).alias("row_seq"),
    )
)

with_windows_promotions = (
    enriched
    .filter(F.col("promo_id") <= 8)
    .select(
        "id",
        "channel",
        "ends_on",
        "promo_id",
        "promotion_discount_pct",
        "next_promotion_discount_pct",
        "seq_no",
        "row_seq",
        F.row_number().over(w8).alias("row_seq_2"),
        F.sum("row_seq").over(w9).alias("promo_code_sum_row_seq"),
        F.first("row_seq", ignorenulls=True).over(w10).alias("first_known_row_seq"),
    )
)

with_window_promotions = (
    with_windows_promotions
    .alias("src")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("src.channel"), F.lit("store")) == F.col("pr.channel")) & F.col("src.ends_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "inner")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

totals = (
    with_window_promotions
    .withColumn("discount_pct_tier", F.when(F.col("discount_pct") >= 0.35, "HIGH").otherwise("LOW"))
    .rollup("discount_pct_tier", "promo_id")
    .agg(
        F.percentile_approx("first_known_row_seq", 0.95).alias("first_known_row_seq_p95"),
        F.avg("row_seq_2").alias("avg_row_seq_2"),
        F.count("*").alias("cnt"),
        F.count("*").alias("cnt_2"),
        F.countDistinct("promo_code").alias("distinct_promo_codes"),
        F.grouping_id().alias("gid"),
    )
)

result = (
    totals
    .select(
        "discount_pct_tier",
        "first_known_row_seq_p95",
        "cnt",
        "cnt_2",
        "distinct_promo_codes",
        "gid",
    )
    .orderBy(F.col("distinct_promo_codes").desc(), "gid")
)

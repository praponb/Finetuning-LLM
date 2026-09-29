from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promotion_promo_code", "channel").orderBy(F.col("discount_pct").desc(), "id", "promo_id")
w2 = Window.partitionBy("channel").orderBy("promo_id", "id", "promo_id")
w3 = Window.orderBy(F.col("promotion_discount_pct").desc_nulls_last())
w4 = Window.partitionBy("promo_code", "channel").orderBy("starts_on", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("promotion_promo_code", "channel").orderBy(F.col("prev_promotion_discount_pct").desc(), "id", "promo_id")
w6 = Window.orderBy("promotion_discount_pct_cume_dist", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("promotion_promo_code").orderBy(F.col("promo_id").desc(), "id", "promo_id")
w8 = Window.partitionBy("promotion_promo_code")
w9 = Window.partitionBy("promo_code")
w10 = Window.partitionBy("channel").orderBy(F.col("discount_pct").desc(), "id", "promo_id")

attributed = (
    df_promotions
    .alias("a")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("a.channel"), F.lit("web")) == F.col("pr.channel")) & (F.col("a.starts_on") >= F.col("pr.starts_on")) & (F.col("a.starts_on") <= F.date_add(F.col("pr.ends_on"), 10)), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

latest = (
    attributed
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

with_windows = (
    latest
    .withColumn("prev_promotion_discount_pct", F.lag("promotion_discount_pct", 2).over(w2))
    .withColumn("promotion_discount_pct_cume_dist", F.cume_dist().over(w3))
    .withColumn("running_promotion_discount_pct", F.sum("promotion_discount_pct").over(w4))
)

metrics = (
    with_windows
    .select(
        "*",
        F.row_number().over(w5).alias("row_seq"),
        F.first("prev_promotion_discount_pct", ignorenulls=True).over(w6).alias("first_known_prev_promotion_discount_pct"),
        F.row_number().over(w7).alias("event_seq"),
        F.stddev_samp("running_promotion_discount_pct").over(w8).alias("promotion_promo_code_stddev_samp_running_promotion_discount_pct"),
        F.min("running_promotion_discount_pct").over(w9).alias("promo_code_min_running_promotion_discount_pct"),
    )
)

latest_promotions = (
    metrics
    .withColumn("latest_rank", F.row_number().over(w10))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

with_window = (
    latest_promotions
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel") == F.col("pr.channel")) & (F.col("s.ends_on") >= F.col("pr.starts_on")) & (F.col("s.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "inner")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

result = (
    with_window
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "promo_id",
        "promotion_discount_pct",
        "dedup_rank",
        "prev_promotion_discount_pct",
        F.when(F.col("channel").rlike("^(mobile|store)$"), "GROUP_B").when(F.col("channel").rlike("^(web|partner)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("channel_zone"),
    )
)

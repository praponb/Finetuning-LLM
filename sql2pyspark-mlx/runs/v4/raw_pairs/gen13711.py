from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("discount_pct").desc(), "id")
w3 = Window.orderBy("starts_on_minus_2d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("promo_code").orderBy("discount_pct")
w5 = Window.orderBy("discount_pct")
w_running = Window.partitionBy("promo_code").orderBy(F.col("running_starts_on_weekofyear").asc_nulls_first())
w7 = Window.partitionBy("channel", "promo_code").orderBy("row_seq", "id")
w8 = Window.partitionBy("promo_code").orderBy("last_known_starts_on_weekofyear", "id").rowsBetween(-5, -1)
w9 = Window.partitionBy("promo_code").orderBy("discount_pct_rank_filled", "id")
w10 = Window.partitionBy("channel", "promo_code").orderBy("discount_pct_rank_filled", "id")
w11 = Window.partitionBy("channel").orderBy("ends_on", "id").rowsBetween(-6, Window.currentRow)

cleaned = (
    df_promotions
    .filter(F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.date_sub("starts_on", 2).alias("starts_on_minus_2d"),
        F.weekofyear("starts_on").alias("starts_on_weekofyear"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("resolved_channel"),
    )
)

enriched = (
    cleaned
    .withColumn("running_starts_on_weekofyear", F.sum("starts_on_weekofyear").over(w))
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("last_known_starts_on_weekofyear", F.last("starts_on_weekofyear", ignorenulls=True).over(w3))
    .withColumn("discount_pct_rank", F.rank().over(w4))
    .withColumn("discount_pct_cume_dist", F.cume_dist().over(w5))
)

valid = (
    enriched
    .filter(F.col("starts_on") >= F.lit("2023-12-14").cast("date"))
    .select(
        "*",
        F.coalesce("discount_pct_rank", F.lit(0)).alias("discount_pct_rank_filled"),
        F.coalesce("channel", F.lit("N/A")).alias("resolved_channel_2"),
    )
)

scored = (
    valid
    .withColumn("running_starts_on_weekofyear_percent_rank", F.percent_rank().over(w_running))
    .withColumn("prev_last_known_starts_on_weekofyear", F.lag("last_known_starts_on_weekofyear", 2, 0).over(w7))
)

enriched_promotions = (
    scored
    .withColumn("discount_pct_cume_dist_avg_5_prior", F.avg("discount_pct_cume_dist").over(w8))
    .withColumn("event_seq", F.row_number().over(w9))
    .withColumn("prev_prev_last_known_starts_on_weekofyear", F.lag("prev_last_known_starts_on_weekofyear", 2).over(w10))
    .withColumn("prev_last_known_starts_on_weekofyear_avg_6", F.avg("prev_last_known_starts_on_weekofyear").over(w11))
)

result = (
    enriched_promotions
    .groupBy("channel")
    .agg(
        F.count(F.when(F.col("channel") == "store", 1)).alias("store_count"),
        F.percentile_approx("prev_last_known_starts_on_weekofyear", 0.5).alias("prev_last_known_starts_on_weekofyear_p50"),
        F.round(F.avg("running_starts_on_weekofyear_percent_rank"), 2).alias("avg_running_starts_on_weekofyear_percent_rank"),
    )
    .orderBy("avg_running_starts_on_weekofyear_percent_rank")
)

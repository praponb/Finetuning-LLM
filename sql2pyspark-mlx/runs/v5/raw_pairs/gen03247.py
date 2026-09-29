from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("discount_pct_scaled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("channel").orderBy(F.col("discount_pct").desc())
w3 = Window.partitionBy("channel")
w_hist = Window.partitionBy("channel").orderBy("discount_pct_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("channel").orderBy("discount_pct_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("channel").orderBy(F.col("first_known_discount_pct_scaled").desc(), "id")
w7 = Window.orderBy("ends_on", "id").rowsBetween(-5, -1)
w_running = Window.partitionBy("channel").orderBy("cumulative_discount_pct_share", "id")

cleaned = (
    df_promotions
    .filter(F.col("channel").isNotNull())
    .select(
        "*",
        F.round((F.col("discount_pct") * 100), 2).alias("discount_pct_scaled"),
        F.last_day("ends_on").alias("ends_on_month_end"),
        F.coalesce("channel", F.lit("DIRECT")).alias("channel_filled"),
    )
)

metrics = (
    cleaned
    .select(
        "*",
        F.first("discount_pct_scaled", ignorenulls=True).over(w).alias("first_known_discount_pct_scaled"),
        F.rank().over(w2).alias("discount_pct_rank"),
        F.round((F.col("discount_pct") / F.sum("discount_pct").over(w3)), 4).alias("discount_pct_share"),
    )
)

metrics_promotions = (
    metrics
    .select(
        "id",
        "channel",
        "ends_on",
        "discount_pct",
        "ends_on_month_end",
        "channel_filled",
        "first_known_discount_pct_scaled",
        F.first("discount_pct_rank", ignorenulls=True).over(w_hist).alias("first_known_discount_pct_rank"),
        F.sum("discount_pct_share").over(w5).alias("cumulative_discount_pct_share"),
    )
)

latest = (
    metrics_promotions
    .withColumn("dedup_rank", F.row_number().over(w6))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

scored = (
    latest
    .select(
        "*",
        F.avg("first_known_discount_pct_rank").over(w7).alias("first_known_discount_pct_rank_avg_5_prior"),
        F.row_number().over(w_running).alias("event_seq"),
        F.min("first_known_discount_pct_scaled").over(w3).alias("channel_min_first_known_discount_pct_scaled"),
    )
)

attributed = (
    scored
    .alias("src")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("src.channel"), F.lit("web")) == F.col("pr.channel")) & (F.col("src.ends_on") >= F.col("pr.starts_on")) & (F.col("src.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

result = (
    attributed
    .withColumn("promo_id_tier", F.when(F.col("promo_id") >= 10, "HIGH").otherwise("LOW"))
    .cube("promo_code", "promo_id_tier")
    .agg(
        F.round(F.sum("first_known_discount_pct_scaled") / F.nullif(F.count(F.when(F.col("promo_code") == "BUNDLE", 1)), F.lit(0)), 2).alias("first_known_discount_pct_scaled_per_bundle"),
        F.sum("promo_id").alias("total_promo_id"),
        F.round(F.avg("event_seq"), 2).alias("avg_event_seq"),
        F.grouping("promo_code").alias("grouping_level"),
    )
    .orderBy(F.col("first_known_discount_pct_scaled_per_bundle").desc_nulls_first())
)

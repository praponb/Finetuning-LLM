from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "promo_code").orderBy("promotion_discount_pct", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("channel").orderBy("discount_pct", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("promotion_promo_code").orderBy("starts_on_period", "id", "promo_id")
w4 = Window.partitionBy("promotion_promo_code").orderBy("starts_on_period", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("channel", "promo_code").orderBy("starts_on", "id", "promo_id", "promo_id_2").rowsBetween(-3, Window.currentRow)
w6 = Window.partitionBy("promotion_promo_code_2").orderBy("discount_pct", "id", "promo_id", "promo_id_2")

filtered = (
    df_promotions
    .filter(F.col("starts_on") > F.to_date(F.lit("2023-08-29")))
    .select(
        "*",
        F.date_trunc("week", "starts_on").alias("starts_on_period"),
        F.coalesce("channel", F.lit("NONE")).alias("resolved_channel"),
    )
)

attributed = (
    filtered
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel") == F.col("pr.channel")) & (F.col("e.starts_on_period") >= F.col("pr.starts_on")) & (F.col("e.starts_on_period") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

with_windows = (
    attributed
    .select(
        "*",
        F.first("promo_id", ignorenulls=True).over(w).alias("first_known_promo_id"),
        F.last("promotion_discount_pct", ignorenulls=True).over(w2).alias("last_known_promotion_discount_pct"),
    )
)

prev_events = with_windows.withColumn("prev_starts_on_period", F.lag("starts_on_period").over(w3))

with_gaps = (
    prev_events
    .withColumn("is_new_session", F.when(F.col("prev_starts_on_period").isNull() | (F.col("starts_on_period") > F.col("prev_starts_on_period") + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_promotions = with_gaps.withColumn("session_id", F.sum("is_new_session").over(w4))

attributed_promotions = (
    sessions_promotions
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("a.ends_on") >= F.col("pr.starts_on")) & (F.col("a.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

enriched = (
    attributed_promotions
    .withColumn("discount_pct_avg_3", F.avg("discount_pct").over(w5))
    .withColumn("prev_promotion_discount_pct_2", F.lag("promotion_discount_pct_2", 1).over(w6))
)

with_window = (
    enriched
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel") == F.col("pr.channel")) & F.col("src.starts_on_period").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id_3"),
        F.col("pr.promo_code").alias("promotion_promo_code_3"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_3"),
    )
)

result = (
    with_window
    .groupBy("promotion_promo_code_2")
    .agg(
        F.percentile_approx("discount_pct_avg_3", [0.25, 0.5, 0.75]).alias("discount_pct_avg_3_quartiles"),
        F.count(F.when(F.col("channel") == "mobile", 1)).alias("mobile_count"),
        F.min("promotion_discount_pct").alias("min_promotion_discount_pct"),
    )
)

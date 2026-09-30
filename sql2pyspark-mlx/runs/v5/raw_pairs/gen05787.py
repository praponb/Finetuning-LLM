from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("starts_on", "id")
w2 = Window.partitionBy("promo_code").orderBy("next_discount_pct", "id", "promo_id")
w3 = Window.orderBy("promotion_discount_pct", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("event_seq", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("promo_code").orderBy("ends_on", "id", "promo_id")
w6 = Window.partitionBy("promo_code").orderBy("prev_discount_pct", "id", "promo_id").rowsBetween(-3, -1)

with_windows = (
    df_promotions
    .select(
        "id",
        "channel",
        "promo_code",
        "ends_on",
        "discount_pct",
        F.lead("discount_pct").over(w).alias("next_discount_pct"),
        F.lag("discount_pct").over(w).alias("prev_discount_pct"),
        F.row_number().over(w).alias("event_seq"),
    )
)

with_window = (
    with_windows
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel").eqNullSafe(F.col("pr.channel"))) & F.col("e.ends_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

base = (
    with_window
    .filter(F.col("promotion_promo_code").isNotNull())
    .select(
        "id",
        "channel",
        "promo_code",
        "ends_on",
        "next_discount_pct",
        "prev_discount_pct",
        "event_seq",
        "promo_id",
        "promotion_promo_code",
        "promotion_discount_pct",
        F.dayofmonth("ends_on").alias("ends_on_day"),
        (F.col("event_seq") * F.col("promo_id")).alias("event_seq_times_promo_id"),
    )
)

metrics = (
    base
    .select(
        "*",
        F.ntile(5).over(w2).alias("next_discount_pct_bucket"),
        F.last("promo_id", ignorenulls=True).over(w3).alias("last_known_promo_id"),
        F.sum("prev_discount_pct").over(w4).alias("running_prev_discount_pct"),
        F.lag("next_discount_pct", 2).over(w5).alias("prev_next_discount_pct"),
        F.avg("prev_discount_pct").over(w6).alias("prev_discount_pct_avg_3_prior"),
    )
)

matched = (
    metrics
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel") == F.col("pr.channel")) & (F.col("s.ends_on") >= F.col("pr.starts_on")) & (F.col("s.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "inner")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct"),
    )
)

result = (
    matched
    .groupBy(
        F.when(F.col("event_seq_times_promo_id") >= 77, "HIGH").otherwise("LOW").alias("event_seq_times_promo_id_tier"),
    )
    .agg(
        F.round(F.sum("event_seq_times_promo_id") / F.nullif(F.count(F.when(F.col("promo_code") == "SPRING10", 1)), F.lit(0)), 2).alias("event_seq_times_promo_id_per_spring10"),
        F.sort_array(F.collect_set("promo_code")).alias("promo_code_list"),
        F.round(F.avg("ends_on_day"), 2).alias("avg_ends_on_day"),
    )
    .filter(F.col("event_seq_times_promo_id_per_spring10") < 1000000)
    .orderBy(F.col("event_seq_times_promo_id_tier").desc())
)

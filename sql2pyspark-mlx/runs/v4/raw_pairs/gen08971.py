from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("ends_on", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("promo_code").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("discount_pct").desc())
w5 = Window.partitionBy("promo_code").orderBy("ends_on", "id", "promo_id").rowsBetween(-6, -1)
w6 = Window.partitionBy("promotion_promo_code", "promo_code").orderBy(F.col("running_discount_pct_2").asc_nulls_last())
w7 = Window.orderBy("discount_pct_cume_dist", "id", "promo_id").rowsBetween(-2, -1)

valid = (
    df_promotions
    .filter((F.col("starts_on") > F.lit("2024-02-18").cast("date")) & F.col("channel").isNotNull())
    .select(
        "*",
        F.datediff(F.current_date(), "starts_on").alias("days_since_starts_on"),
    )
)

metrics = (
    valid
    .select(
        "*",
        F.avg("days_since_starts_on").over(w).alias("days_since_starts_on_avg_3"),
        F.sum("discount_pct").over(w2).alias("running_discount_pct"),
        F.sum("discount_pct").over(w3).alias("running_discount_pct_2"),
    )
)

with_windows = (
    metrics
    .select(
        "*",
        F.first("channel", ignorenulls=True).over(w2).alias("first_known_channel"),
        F.cume_dist().over(w4).alias("discount_pct_cume_dist"),
    )
)

with_window = (
    with_windows
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel") == F.col("pr.channel")) & (F.col("a.starts_on") >= F.col("pr.starts_on")) & (F.col("a.starts_on") <= F.date_add(F.col("pr.ends_on"), 7)), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

valid_promotions = (
    with_window
    .filter(F.col("promotion_promo_code").isNotNull() & ~F.col("promotion_promo_code").isin(["BUNDLE", "SPRING10"]))
    .select(
        "*",
        (F.col("days_since_starts_on_avg_3") - F.col("promotion_discount_pct")).alias("days_since_starts_on_avg_3_minus_promotion_discount_pct"),
    )
)

metrics_promotions = (
    valid_promotions
    .select(
        "*",
        F.avg("days_since_starts_on").over(w5).alias("days_since_starts_on_avg_6_prior"),
        F.dense_rank().over(w6).alias("running_discount_pct_2_dense_rank"),
        F.avg("days_since_starts_on_avg_3").over(w7).alias("days_since_starts_on_avg_3_avg_2_prior"),
    )
)

with_window_promotions = (
    metrics_promotions
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("s.ends_on") >= F.col("pr.starts_on")) & (F.col("s.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "inner")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

result = (
    with_window_promotions
    .select(
        "id",
        "starts_on",
        "ends_on",
        "discount_pct",
        "days_since_starts_on_avg_3",
        "running_discount_pct",
        "running_discount_pct_2",
        "discount_pct_cume_dist",
        "promo_id",
    )
    .orderBy("running_discount_pct")
)

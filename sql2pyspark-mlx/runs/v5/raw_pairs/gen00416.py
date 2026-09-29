from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code")
w2 = Window.orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("promo_code").orderBy("cumulative_starts_on_day")

base = (
    df_promotions
    .filter(F.col("channel").isNotNull() & F.month("starts_on").isin([3, 9, 12]))
    .select(
        "*",
        F.when(F.col("discount_pct") >= 0.3, "High").when(F.col("discount_pct") >= 0.14, "Medium").otherwise("Low").alias("discount_pct_tier"),
        F.dayofmonth("starts_on").alias("starts_on_day"),
        F.coalesce("channel", F.lit("NONE")).alias("channel_filled"),
    )
)

metrics = (
    base
    .withColumn("promo_code_min_discount_pct", F.min("discount_pct").over(w))
    .withColumn("cumulative_starts_on_day", F.sum("starts_on_day").over(w2))
)

remaining_keys = (
    metrics
    .select("id", "channel")
    .subtract(
        df_promotions
        .filter(F.col("promo_code") == "WELCOME5")
        .select("id", "channel")
    )
)

reconciled = (
    metrics
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

with_windows = (
    reconciled
    .select(
        "*",
        F.percent_rank().over(w_running).alias("cumulative_starts_on_day_percent_rank"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("cumulative_starts_on_day") >= 418, "LARGE").otherwise("SMALL").alias("cumulative_starts_on_day_tier"),
    )
    .agg(
        F.sum(F.when(F.col("promo_code") == "WELCOME5", F.col("cumulative_starts_on_day_percent_rank"))).alias("welcome5_total_cumulative_starts_on_day_percent_rank"),
        F.round(F.sum("starts_on_day") / F.nullif(F.count(F.when(F.col("channel") == "mobile", 1)), F.lit(0)), 2).alias("starts_on_day_per_mobile"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.sum("cumulative_starts_on_day") / F.nullif(F.count(F.when(F.col("promo_code") == "WELCOME5", 1)), F.lit(0)), 2).alias("cumulative_starts_on_day_per_welcome5"),
    )
    .orderBy("welcome5_total_cumulative_starts_on_day_percent_rank")
)

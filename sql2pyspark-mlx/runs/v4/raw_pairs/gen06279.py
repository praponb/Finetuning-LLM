from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_limits").alias("val"),
    )
)

cleaned = (
    lines
    .filter(F.col("effective_from") > F.to_date(F.lit("2024-06-07")))
    .select(
        "id",
        "plan",
        "effective_from",
        "val",
        F.round("val", 2).alias("val_rounded"),
    )
)

remaining_keys = (
    cleaned
    .select("id")
    .intersectAll(
        df_price_tiers
        .filter(F.col("effective_from") <= "2025-08-30")
        .select("id")
    )
)

kept_rows = (
    cleaned
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

metrics = (
    kept_rows
    .withColumn("plan_max_val_rounded", F.max("val_rounded").over(w))
    .withColumn("plan_avg_val_rounded", F.avg("val_rounded").over(w))
    .withColumn("plan_stddev_samp_val", F.stddev_samp("val").over(w))
    .withColumn("plan_stddev_samp_val_rounded", F.stddev_samp("val_rounded").over(w))
)

daily = (
    metrics
    .groupBy(
        F.date_trunc("MONTH", "effective_from").alias("activity_month"),
        F.date_trunc("WEEK", "effective_from").alias("week_start"),
        F.when(F.col("plan_avg_val_rounded") >= 81, "LARGE").otherwise("SMALL").alias("plan_avg_val_rounded_tier"),
    )
    .agg(
        F.sum(F.when(F.col("plan") == "enterprise", F.col("val")).otherwise(0)).alias("enterprise_val"),
        F.sum(F.when(F.col("plan") == "starter", F.col("plan_stddev_samp_val"))).alias("starter_total_plan_stddev_samp_val"),
    )
)

result = (
    daily
    .select(
        "activity_month",
        "week_start",
        "plan_avg_val_rounded_tier",
        "enterprise_val",
        "starter_total_plan_stddev_samp_val",
    )
)

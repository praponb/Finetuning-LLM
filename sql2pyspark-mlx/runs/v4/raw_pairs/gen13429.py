from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("subscription_id_2_tier").orderBy(F.col("subscription_monthly_fee_per_jp").desc())

with_window = (
    df_sessions
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & (F.col("a.started_at") >= F.col("sub.start_date")) & (F.col("a.started_at") < F.col("sub.end_date") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

attributed = (
    with_window
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & F.col("a.started_at").between(F.col("sub.start_date"), F.col("sub.end_date")), "left")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id_2"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

remaining_keys = (
    attributed
    .select("device")
    .subtract(
        df_sessions
        .filter(F.col("device").isin(["desktop", "mobile"]))
        .select("device")
    )
)

reconciled = (
    attributed
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.device") == F.col("k.device")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        F.when(F.col("subscription_id_2") >= 10, "PREMIUM").otherwise("STANDARD").alias("subscription_id_2_tier"),
        F.date_trunc("quarter", "started_at").alias("quarter_start"),
    )
    .agg(
        F.round(F.sum("subscription_id_2") / F.nullif(F.count(F.when(F.col("browser") == "Firefox", 1)), F.lit(0)), 2).alias("subscription_id_2_per_firefox"),
        F.round(F.sum("subscription_monthly_fee") / F.nullif(F.count(F.when(F.col("country") == "JP", 1)), F.lit(0)), 2).alias("subscription_monthly_fee_per_jp"),
        F.percentile_approx("subscription_id", 0.9).alias("subscription_id_p90"),
    )
    .withColumn("subscription_monthly_fee_per_jp_dense_rank", F.dense_rank().over(w))
    .orderBy("subscription_monthly_fee_per_jp")
)

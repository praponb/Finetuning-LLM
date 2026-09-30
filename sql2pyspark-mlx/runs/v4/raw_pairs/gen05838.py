from pyspark.sql import functions as F

kept_keys = (
    df_subscriptions
    .select("customer_id", "id")
    .exceptAll(
        df_subscriptions
        .filter(F.col("customer_id").isin([1, 8, 25, 26]))
        .select("customer_id", "id")
    )
)

reconciled = (
    df_subscriptions
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        F.when(F.col("monthly_fee") >= 233, "LARGE").otherwise("SMALL").alias("monthly_fee_tier"),
    )
    .agg(
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"),
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.count(F.when(F.col("status") == "PAUSED", 1)).alias("paused_rows"),
        F.round(F.sum("monthly_fee") / F.nullif(F.count(F.when(F.col("status") == "PAUSED", 1)), F.lit(0)), 2).alias("monthly_fee_per_paused"),
        F.count(F.when(F.col("plan") == "premium", 1)).alias("premium_count"),
        F.sum(F.when(F.col("status") == "PAUSED", F.col("monthly_fee")).otherwise(0)).alias("paused_monthly_fee"),
    )
    .orderBy("avg_monthly_fee")
)

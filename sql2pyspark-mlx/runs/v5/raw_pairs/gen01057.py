from pyspark.sql import functions as F

with_window = (
    df_accounts
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & F.col("a.opened_date").between(F.col("sub.start_date"), F.col("sub.end_date")), "inner")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

result = (
    with_window
    .groupBy("customer_id")
    .agg(
        F.percentile_approx("subscription_id", 0.9).alias("subscription_id_p90"),
        F.round(F.sum("balance") / F.nullif(F.count(F.when(F.col("plan") == "standard", 1)), F.lit(0)), 2).alias("balance_per_standard"),
        F.avg(F.when(F.col("plan") == "enterprise", F.col("monthly_fee"))).alias("enterprise_avg_monthly_fee"),
    )
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "account_type").orderBy("balance", "id")
w2 = Window.partitionBy("account_type").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_accounts
    .filter((F.col("opened_date") >= F.lit("2023-02-28").cast("date")) & F.col("account_type").isin(["LOAN", "SAVINGS", "CHECKING"]))
    .withColumn("balance_int", F.col("balance").cast("int"))
    .withColumn("status_filled", F.coalesce("status", F.lit("N/A")))
)

not_flagged = (
    filtered
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.opened_date")), "left_semi")
)

with_windows = (
    not_flagged
    .select(
        "*",
        F.lag("balance", 2).over(w).alias("prev_balance"),
        F.sum("balance").over(w2).alias("running_balance"),
    )
)

matched = (
    with_windows
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & (F.col("a.opened_date") >= F.col("sub.start_date")) & (F.col("a.opened_date") < F.col("sub.end_date") + F.expr("INTERVAL 7 DAYS")), "inner")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

kept_keys = (
    matched
    .select("id")
    .intersectAll(
        df_accounts
        .filter(F.col("balance").between(42000, 76000))
        .select("id")
    )
)

reconciled = (
    matched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        F.when(F.col("subscription_id") >= 11, "LARGE").otherwise("SMALL").alias("subscription_id_tier"),
        "subscription_id",
    )
    .agg(
        F.count(F.when(F.col("account_type") == "CHECKING", 1)).alias("checking_count"),
        F.count_if(F.col("status") == "FROZEN").alias("frozen_events"),
        F.percentile_approx("prev_balance", 0.9).alias("prev_balance_p90"),
        F.countDistinct("plan").alias("distinct_plans"),
        F.count(F.when(F.col("plan") == "premium", 1)).alias("premium_rows"),
    )
    .filter(F.col("distinct_plans") >= 1)
)

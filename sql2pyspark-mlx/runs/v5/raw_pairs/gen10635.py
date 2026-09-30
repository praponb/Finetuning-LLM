from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("account_type", "customer_id")
w_grp = Window.partitionBy("status").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status", "account_type").orderBy("account_type_sum_balance", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("account_type").orderBy("subscription_id_num", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("account_type").orderBy(F.col("running_sum_first_known_balance").desc())

metrics = (
    df_accounts
    .filter(~F.col("account_type").isin(["SAVINGS", "CHECKING"]))
    .select(
        "*",
        F.sum("balance").over(w_ordered).alias("cumulative_balance"),
        F.sum("balance").over(w2).alias("account_type_sum_balance"),
        F.first("balance", ignorenulls=True).over(w_ordered).alias("first_known_balance"),
        F.sum("balance").over(w_grp).alias("running_balance"),
    )
)

with_window = (
    metrics
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & F.col("a.opened_date").between(F.col("sub.start_date"), F.col("sub.end_date")), "inner")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

filtered = (
    with_window
    .filter((F.col("status").eqNullSafe(F.lit("OPEN"))) & F.col("plan").isNotNull())
    .select(
        "id",
        "account_type",
        "status",
        "cumulative_balance",
        "account_type_sum_balance",
        "first_known_balance",
        "running_balance",
        "subscription_id",
        F.lower("account_type").alias("account_type_lower"),
        F.coalesce("status", F.lit("N/A")).alias("status_clean"),
    )
)

with_windows = (
    filtered
    .select(
        "id",
        "account_type",
        "cumulative_balance",
        "subscription_id",
        "account_type_lower",
        F.avg("first_known_balance").over(w4).alias("running_avg_first_known_balance"),
        F.max("first_known_balance").over(w4).alias("running_max_first_known_balance"),
        F.sum("first_known_balance").over(w4).alias("running_sum_first_known_balance"),
        F.min("first_known_balance").over(w4).alias("running_min_first_known_balance"),
    )
)

filtered_accounts = (
    with_windows
    .filter(F.col("running_avg_first_known_balance") >= 35000)
    .select(
        "id",
        "account_type",
        "subscription_id",
        "running_avg_first_known_balance",
        "running_max_first_known_balance",
        "running_sum_first_known_balance",
        "running_min_first_known_balance",
        F.col("subscription_id").cast("double").alias("subscription_id_num"),
    )
)

enriched = (
    filtered_accounts
    .select(
        "id",
        "subscription_id",
        "running_max_first_known_balance",
        "running_sum_first_known_balance",
        "running_min_first_known_balance",
        "subscription_id_num",
        F.sum("subscription_id_num").over(w5).alias("cumulative_subscription_id_num"),
        F.cume_dist().over(w6).alias("running_sum_first_known_balance_cume_dist"),
    )
)

not_flagged = (
    enriched
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_type").isin(["FEE", "REFUND", "CREDIT"])), "left_semi")
)

result = (
    not_flagged
    .groupBy(
        F.when(F.col("subscription_id_num") >= 65, "LARGE").otherwise("SMALL").alias("subscription_id_num_tier"),
        F.when(F.col("cumulative_subscription_id_num") >= 339, "LARGE").otherwise("SMALL").alias("cumulative_subscription_id_num_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("cnt"),
        F.round(F.avg("running_min_first_known_balance"), 2).alias("avg_running_min_first_known_balance"),
        F.percentile_approx("running_min_first_known_balance", 0.95).alias("running_min_first_known_balance_p95"),
    )
)

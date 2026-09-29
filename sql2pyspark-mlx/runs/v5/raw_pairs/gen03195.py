from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.partitionBy("plan").orderBy("subscription_id")
w3 = Window.partitionBy("account_type")
w4 = Window.partitionBy("customer_id")
w_grp = Window.orderBy("subscription_id_cume_dist", "id", "subscription_id").rowsBetween(-2, Window.currentRow)
w6 = Window.partitionBy("customer_id").orderBy("balance", "id", "subscription_id")
w7 = Window.partitionBy("customer_id").orderBy("customer_id_max_subscription_id", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("customer_id").orderBy("balance", "id", "subscription_id").rowsBetween(-2, -1)
w9 = Window.partitionBy("customer_id").orderBy(F.col("account_type_stddev_samp_balance_rounded").desc())
w10 = Window.partitionBy("customer_id").orderBy(F.col("subscription_id_avg_2").desc(), "id", "subscription_id")
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.min("monthly_fee").alias("min_subscription_monthly_fee"))

filtered = (
    df_accounts
    .filter((F.col("opened_date") >= F.lit("2023-06-25").cast("date")) & F.col("status").isNotNull())
    .select("*", F.round("balance", 0).alias("balance_rounded"))
)

matched = (
    filtered
    .alias("e")
    .join(df_subscriptions.alias("sub"), (F.col("e.customer_id") == F.col("sub.customer_id")) & (F.col("e.opened_date") >= F.col("sub.start_date")) & (F.col("e.opened_date") < F.col("sub.end_date") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "e.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

with_windows = (
    matched
    .filter(~F.col("status").isin(["OPEN", "CLOSED"]))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "balance_rounded",
        "subscription_id",
        F.stddev_samp("monthly_fee").over(w).alias("plan_stddev_samp_monthly_fee"),
        F.cume_dist().over(w2).alias("subscription_id_cume_dist"),
        F.stddev_samp("balance_rounded").over(w3).alias("account_type_stddev_samp_balance_rounded"),
    )
)

with_windows_accounts = (
    with_windows
    .select(
        "id",
        "customer_id",
        "balance",
        "balance_rounded",
        "subscription_id",
        "account_type_stddev_samp_balance_rounded",
        F.count("balance").over(w3).alias("account_type_count_balance"),
        F.max("subscription_id").over(w4).alias("customer_id_max_subscription_id"),
        F.avg("subscription_id").over(w_grp).alias("subscription_id_avg_2"),
    )
)

metrics = (
    with_windows_accounts
    .select(
        "id",
        "customer_id",
        "balance",
        "balance_rounded",
        "subscription_id",
        "customer_id_max_subscription_id",
        F.lead("balance_rounded", 2).over(w6).alias("next_balance_rounded"),
        F.sum("account_type_count_balance").over(w7).alias("running_account_type_count_balance"),
        F.avg("subscription_id_avg_2").over(w8).alias("subscription_id_avg_2_avg_2_prior"),
        F.percent_rank().over(w9).alias("account_type_stddev_samp_balance_rounded_percent_rank"),
        F.ntile(10).over(w10).alias("subscription_id_avg_2_bucket"),
    )
)

result = (
    metrics
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "subscription_id",
        "customer_id_max_subscription_id",
        "next_balance_rounded",
        "running_account_type_count_balance",
        "subscription_id_avg_2_avg_2_prior",
        "account_type_stddev_samp_balance_rounded_percent_rank",
        "subscription_id_avg_2_bucket",
        F.col("min_subscription_monthly_fee"),
    )
)

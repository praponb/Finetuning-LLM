from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("monthly_fee").desc(), "id", "subscription_id")
w2 = Window.partitionBy("account_type").orderBy(F.col("adjusted_balance").desc_nulls_first())
w3 = Window.partitionBy("plan", "status").orderBy("adjusted_balance", "id", "subscription_id")
w4 = Window.partitionBy("plan").orderBy(F.col("balance_int").desc_nulls_last(), "id", "subscription_id")

filtered = (
    df_accounts
    .filter(F.col("customer_id").isNotNull() & (F.col("status") == "CLOSED"))
    .select(
        "*",
        F.col("balance").cast("int").alias("balance_int"),
        F.round((F.col("balance") * 1.07), 2).alias("adjusted_balance"),
        F.coalesce("status", F.lit("N/A")).alias("resolved_status"),
    )
)

attributed = (
    filtered
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & F.col("a.opened_date").between(F.col("sub.start_date"), F.col("sub.end_date")), "left")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

metrics = (
    attributed
    .select(
        "*",
        F.ntile(4).over(w).alias("monthly_fee_bucket"),
        F.cume_dist().over(w2).alias("adjusted_balance_cume_dist"),
        F.row_number().over(w3).alias("seq_no"),
    )
)

latest = (
    metrics
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "account_type",
        "resolved_status",
        "subscription_id",
        "adjusted_balance_cume_dist",
    )
)

not_flagged = (
    latest
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.merchant").isNull()), "left_anti")
)

filtered_accounts = (
    not_flagged
    .filter(F.col("adjusted_balance_cume_dist") > 0.69)
    .select(
        "id",
        "account_type",
        "resolved_status",
        "subscription_id",
        "adjusted_balance_cume_dist",
        F.round((F.col("adjusted_balance_cume_dist") / F.col("subscription_id")), 2).alias("adjusted_balance_cume_dist_per_subscription_id"),
    )
)

eligible = (
    filtered_accounts
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.is_flagged")), "left_anti")
)

result = (
    eligible
    .select(
        "id",
        "account_type",
        "subscription_id",
        "adjusted_balance_cume_dist",
        "adjusted_balance_cume_dist_per_subscription_id",
    )
    .orderBy("adjusted_balance_cume_dist")
)

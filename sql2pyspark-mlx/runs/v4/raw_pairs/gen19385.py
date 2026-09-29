from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("merchant").orderBy("subscription_id", "id", "subscription_id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy("balance", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_accounts
    .filter(F.col("opened_date") <= "2025-04-01")
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        F.round((F.col("balance") * 0.9), 2).alias("adjusted_balance"),
    )
)

with_latest = (
    valid
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.opened_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.txn_time").alias("transaction_txn_time"),
        F.col("c.merchant"),
    )
)

eligible = (
    with_latest
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") < F.col("s.opened_date")), "left_anti")
)

matched = (
    eligible
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & (F.col("src.opened_date") >= F.col("sub.start_date")) & (F.col("src.opened_date") <= F.date_add(F.col("sub.end_date"), 10)), "left")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

scored = (
    matched
    .select(
        "*",
        F.avg("balance").over(w2).alias("balance_avg_3"),
        F.last("balance", ignorenulls=True).over(w3).alias("last_known_balance"),
    )
)

not_flagged = (
    scored
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_time") <= F.col("s.transaction_txn_time")), "left_anti")
)

result = (
    not_flagged
    .rollup("customer_id")
    .agg(
        F.percentile_approx("last_known_balance", 0.9).alias("last_known_balance_p90"),
        F.sort_array(F.collect_set("account_type")).alias("account_type_list"),
    )
)

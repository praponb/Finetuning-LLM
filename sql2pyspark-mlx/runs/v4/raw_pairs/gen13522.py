from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

valid = (
    df_accounts
    .filter(F.col("status").isin(["FROZEN", "OPEN", "CLOSED"]))
    .withColumn("clean_account_type", F.trim("account_type"))
    .withColumn("days_since_opened_date", F.datediff(F.current_date(), "opened_date"))
)

eligible = (
    valid
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.txn_time") < F.col("s.opened_date")), "left_anti")
)

joined = (
    eligible
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "inner")
    .select("src.*", F.col("p.credit_limit"))
)

enriched = (
    joined
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "left")
    .select("e.*", F.col("c.city"))
)

eligible_accounts = (
    enriched
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.to_date(F.col("c.txn_time")) >= "2024-03-01"), "left_semi")
)

with_last = (
    eligible_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.amount").alias("transaction_amount"),
        F.col("c.txn_type").alias("transaction_txn_type"),
    )
)

attributed = (
    with_last
    .alias("s")
    .join(df_subscriptions.alias("sub"), (F.col("s.customer_id") == F.col("sub.customer_id")) & (F.col("s.opened_date") >= F.col("sub.start_date")) & (F.col("s.opened_date") <= F.date_add(F.col("sub.end_date"), 10)), "left")
    .select(
        "s.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

result = (
    attributed
    .select(
        "id",
        "subscription_id",
        "status",
        F.col("transaction_amount").alias("TRANSACTION_AMOUNT_VAL"),
        F.col("credit_limit").alias("CREDIT_LIMIT"),
        F.col("monthly_fee").alias("MONTHLY_FEE"),
    )
    .unpivot(
        ["id", "subscription_id", "status"],
        ["TRANSACTION_AMOUNT_VAL", "CREDIT_LIMIT", "MONTHLY_FEE"],
        "metric",
        "value",
    )
)

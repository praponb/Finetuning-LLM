from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("status", "account_type").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("transaction_merchant").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
w5 = Window.partitionBy("transaction_txn_type").orderBy("txn_time", "id")
w6 = Window.partitionBy("transaction_txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_accounts
    .filter(F.col("customer_id").isNotNull() & (F.col("account_type") == "CREDIT"))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.lower("status").alias("status_lower"),
    )
)

with_latest = (
    base
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.txn_type").alias("transaction_txn_type"),
        F.col("c.merchant").alias("transaction_merchant"),
    )
)

with_dims = (
    with_latest
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id") == F.col("b.id"), "left")
    .filter(F.col("b.segment").isin(["Corporate", "Consumer"]))
    .select(
        "src.id",
        "src.customer_id",
        "src.account_type",
        "src.balance",
        "src.status",
        "src.transaction_txn_type",
        "src.transaction_merchant",
        F.col("b.name"),
        F.col("b.signup_date"),
        F.col("b.credit_limit"),
    )
)

enriched = (
    with_dims
    .select(
        "*",
        F.sum("balance").over(w2).alias("running_balance"),
        F.sum("credit_limit").over(w3).alias("running_credit_limit"),
    )
)

filtered = (
    enriched
    .filter(F.col("customer_id").isNotNull())
    .withColumn("name_filled", F.coalesce("name", F.lit("N/A")))
    .withColumn("balance_times_running_credit_limit", (F.col("balance") * F.col("running_credit_limit")))
)

with_latest_accounts = (
    filtered
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.signup_date")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.account_type",
        "s.balance",
        "s.status",
        "s.transaction_txn_type",
        "s.name",
        "s.signup_date",
        "s.credit_limit",
        "s.running_balance",
        "s.running_credit_limit",
        F.col("c.txn_time"),
        F.col("c.txn_type").alias("transaction_txn_type_2"),
    )
)

prev_events = with_latest_accounts.withColumn("prev_txn_time", F.lag("txn_time").over(w5))

with_gaps = (
    prev_events
    .withColumn("session_start_flag", F.when(F.col("prev_txn_time").isNull() | (F.col("txn_time") > F.col("prev_txn_time") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("burst_id", F.sum("session_start_flag").over(w6))

result = (
    sessionized
    .select(
        "id",
        "account_type",
        "balance",
        "name",
        "signup_date",
        "credit_limit",
        "running_balance",
        "transaction_txn_type_2",
        "prev_txn_time",
        F.when(F.col("name").rlike("^(FRANK MILLER|Jack Taylor)$"), "CORE").otherwise("PRIORITY").alias("name_zone"),
    )
)

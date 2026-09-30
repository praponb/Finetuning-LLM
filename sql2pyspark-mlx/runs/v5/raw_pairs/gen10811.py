from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("opened_date_period", "id")
w2 = Window.partitionBy("status").orderBy("opened_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())

cleaned = (
    df_accounts
    .filter((F.col("opened_date") > F.lit("2023-10-01").cast("date")) & F.col("customer_id").isNotNull())
    .withColumn("opened_date_period", F.date_trunc("month", "opened_date"))
    .withColumn("resolved_status", F.coalesce("status", F.lit("NONE")))
)

not_flagged = (
    cleaned
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.account_id").isin([19, 32])), "left_anti")
)

kept = (
    not_flagged
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_time") <= F.col("s.opened_date")), "left_anti")
)

prev_events = kept.withColumn("prev_opened_date_period", F.lag("opened_date_period").over(w))

flagged = (
    prev_events
    .withColumn("new_group_flag", F.when(F.col("prev_opened_date_period").isNull() | (F.col("opened_date_period") > F.col("prev_opened_date_period") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_accounts = flagged.withColumn("session_id", F.sum("new_group_flag").over(w2))

with_last = (
    sessions_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.account_type",
        "s.balance",
        "s.status",
        "s.resolved_status",
        "s.new_group_flag",
        "s.session_id",
        F.col("c.amount").alias("transaction_amount"),
        F.col("c.txn_type"),
    )
)

not_flagged_accounts = (
    with_last
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.is_flagged")), "left_anti")
)

cleaned_accounts = (
    not_flagged_accounts
    .filter(F.col("status") == "OPEN")
    .select(
        "id",
        "customer_id",
        "account_type",
        "status",
        "resolved_status",
        "session_id",
        "transaction_amount",
        "txn_type",
        F.when(F.col("account_type") == "CREDIT", 1).otherwise(0).alias("is_credit"),
    )
)

result = (
    cleaned_accounts
    .groupBy(
        F.when(F.col("transaction_amount") >= 4300, "LARGE").otherwise("SMALL").alias("transaction_amount_tier"),
        "account_type",
    )
    .agg(
        F.count_if(F.col("status") == "CLOSED").alias("closed_events"),
        F.round(F.avg("is_credit"), 2).alias("avg_is_credit"),
        F.count_if(F.col("account_type") == "SAVINGS").alias("savings_events"),
        F.count(F.when(F.col("status") == "FROZEN", 1)).alias("frozen_rows"),
        F.countDistinct("status").alias("distinct_status"),
        F.countDistinct("account_type").alias("distinct_account_types"),
    )
    .orderBy(F.col("distinct_account_types").desc())
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("transaction_txn_type").orderBy("txn_time", "id")
w3 = Window.partitionBy("transaction_txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_last = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.merchant"),
        F.col("c.txn_type").alias("transaction_txn_type"),
    )
)

with_last_accounts = (
    with_last
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.txn_time"))
)

kept = (
    with_last_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_type") == "CREDIT"), "left_anti")
)

with_gaps = (
    kept
    .withColumn("session_start_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w2) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "balance",
        "opened_date",
        "status",
        "transaction_txn_type",
        "txn_time",
        F.sum("session_start_flag").over(w3).alias("session_no"),
    )
)

remaining_keys = (
    islands
    .select("id")
    .exceptAll(
        df_accounts
        .filter(F.col("status") == "CLOSED")
        .select("id")
    )
)

verified = (
    islands
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    verified
    .filter(F.col("status").isNotNull())
    .select(
        "balance",
        "status",
        "session_no",
        F.datediff(F.current_date(), "opened_date").alias("days_since_opened_date"),
    )
)

cleaned = (
    base
    .filter(F.col("session_no") > 6)
    .withColumn("least_days_since_opened_date_balance", F.least("days_since_opened_date", "balance"))
    .withColumn("days_since_opened_date_rounded", F.round("days_since_opened_date", 2))
    .withColumn("status_clean", F.coalesce("status", F.lit("OTHER")))
)

result = (
    cleaned
    .select(
        "status",
        F.col("least_days_since_opened_date_balance").alias("LEAST_DAYS_SINCE_OPENED_DATE_BALANCE"),
        F.col("days_since_opened_date_rounded").alias("DAYS_SINCE_OPENED_DATE_ROUNDED"),
        F.col("balance").alias("BALANCE"),
    )
    .unpivot(
        ids=["status"],
        values=["LEAST_DAYS_SINCE_OPENED_DATE_BALANCE", "DAYS_SINCE_OPENED_DATE_ROUNDED", "BALANCE"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)

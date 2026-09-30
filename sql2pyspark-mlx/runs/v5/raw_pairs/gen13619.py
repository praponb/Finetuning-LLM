from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("balance").desc(), "id")
w3 = Window.partitionBy("customer_id", "account_type").orderBy("balance_bucket", "id")
w4 = Window.partitionBy("account_type").orderBy("opened_date", "id")
w5 = Window.partitionBy("customer_id").orderBy("balance", "id")
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w7 = Window.partitionBy("transaction_txn_type").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("transaction_txn_type")

with_windows = (
    df_accounts
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        F.sum("balance").over(w).alias("running_sum_balance"),
        F.min("balance").over(w).alias("running_min_balance"),
        F.avg("balance").over(w).alias("running_avg_balance"),
        F.ntile(10).over(w2).alias("balance_bucket"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "balance",
        "opened_date",
        "running_sum_balance",
        "balance_bucket",
        F.row_number().over(w3).alias("row_seq"),
        F.row_number().over(w4).alias("seq_no"),
        F.lag("running_sum_balance", 1).over(w5).alias("prev_running_sum_balance"),
    )
)

with_last = (
    metrics
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.txn_time").alias("transaction_txn_time"),
        F.col("c.txn_type").alias("transaction_txn_type"),
    )
)

metrics_accounts = (
    with_last
    .filter(F.col("transaction_txn_type").isin(["CREDIT", "DEBIT", "REFUND"]))
    .withColumn("running_min_running_sum_balance", F.min("running_sum_balance").over(w7))
    .withColumn("running_max_running_sum_balance", F.max("running_sum_balance").over(w7))
    .withColumn("running_sum_running_sum_balance", F.sum("running_sum_balance").over(w7))
    .withColumn("seq_no_share", F.round((F.col("seq_no") / F.sum("seq_no").over(w8)), 4))
)

result = (
    metrics_accounts
    .select(
        "id",
        "transaction_txn_type",
        F.col("seq_no_share").alias("SEQ_NO_SHARE_VAL"),
        F.col("running_min_running_sum_balance").alias("RUNNING_MIN_RUNNING_SUM_BALANCE"),
        F.col("running_max_running_sum_balance").alias("RUNNING_MAX_RUNNING_SUM_BALANCE"),
        F.col("running_sum_balance").alias("RUNNING_SUM_BALANCE"),
    )
    .unpivot(
        ids=["id", "transaction_txn_type"],
        values=["SEQ_NO_SHARE_VAL", "RUNNING_MIN_RUNNING_SUM_BALANCE", "RUNNING_MAX_RUNNING_SUM_BALANCE", "RUNNING_SUM_BALANCE"],
        variableColumnName="metric",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
